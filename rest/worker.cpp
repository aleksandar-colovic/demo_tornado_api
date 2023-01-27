#include <iostream>
#include <string.h>
#include <algorithm>
#include <thread>
#include <chrono>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <cassandra.h>
#include <hiredis/hiredis.h>

int getComments(CassSession* session, CassFuture* connect_future, int video_id, std::string& comments)
{
    // read comments from cassandra
    std::string replyMessage;
    if (cass_future_error_code(connect_future) == CASS_OK)
    {
      std::string query("SELECT * FROM comments.comments_by_video WHERE videoid = ");
      query += std::to_string(video_id);
      CassStatement* statement = cass_statement_new(query.c_str(), 0);
      CassFuture *result_future = cass_session_execute(session, statement);
      if (cass_future_error_code(result_future) == CASS_OK)
      {
        const CassResult *result = cass_future_get_result(result_future);
        CassIterator *iterator = cass_iterator_from_result(result);
        char str[CASS_UUID_STRING_LENGTH];
        
        while (cass_iterator_next(iterator))
        {
          const CassRow *row = cass_iterator_get_row(iterator);

          const char *comment;
          size_t length;
          const CassValue* value = cass_row_get_column_by_name(row, "comment");
          cass_value_get_string(value, &comment, &length);
          comments += (comments.empty() ? "" : "; ") + std::string(comment, length);
        }

        cass_iterator_free(iterator);
        cass_result_free(result);
      }
      else
      {
        std::cerr << "Unable to run query" << std::endl;
        return 1;
      }

      cass_statement_free(statement);
      cass_future_free(result_future);
    }
    else
    {
      std::cerr << "Unable to connect to cassandra" << std::endl;
      return 1;
    }

  return 0;
}

int main(int argc, char const *const *argv)
{

  amqp_connection_state_t conn = amqp_new_connection();
  amqp_socket_t *socket = amqp_tcp_socket_new(conn);
  amqp_socket_open(socket, "localhost", AMQP_PROTOCOL_PORT);

  amqp_login(conn, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
  const amqp_channel_t KChannel = 1;
  amqp_channel_open(conn, KChannel);

  amqp_bytes_t queueName(amqp_cstring_bytes("task_queue"));
  amqp_queue_declare(conn, KChannel, queueName, false, /*durable*/ false, false, false, amqp_empty_table);

  amqp_basic_qos(conn, KChannel, 0, /*prefetch_count*/ 1, 0);
  amqp_basic_consume(conn, KChannel, queueName, amqp_empty_bytes, false, /* auto ack*/ false, false, amqp_empty_table);

  // setup cassandra
  CassFuture *connect_future = NULL;
  CassCluster *cluster = cass_cluster_new();
  CassSession *session = cass_session_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  connect_future = cass_session_connect(session, cluster);
  // setup redis
  redisContext* redisCtxt = redisConnect("127.0.0.1", 6379);
  if (redisCtxt == NULL || redisCtxt->err)
  {
      if (redisCtxt)
      {
        printf("Redis connection error: %s\n", redisCtxt->errstr);
        redisFree(redisCtxt);
        redisCtxt = nullptr;
      }
      else
      {
        printf("Connection error: can't allocate redis context\n");
      }
  }

  for (;;)
  {
    amqp_maybe_release_buffers(conn);
    amqp_envelope_t envelope;
    amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, nullptr, 0);

    std::cout << " Worker processing a request: " << std::string((char *)envelope.message.properties.correlation_id.bytes, envelope.message.properties.correlation_id.len) << std::endl;
    std::string vid((char*)envelope.message.body.bytes, (int)envelope.message.body.len);
    int video_id = std::stoi(vid.c_str());
    std::string comments;
    int ok = getComments(session, connect_future, video_id, comments);
    if (ok != 0)
      return ok;

    // cache to redis
    redisReply* reply = (redisReply*)redisCommand(redisCtxt,"SET comments_%s %s", vid.c_str(), comments.c_str());
    freeReplyObject(reply);

    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CORRELATION_ID_FLAG;
    props.correlation_id = envelope.message.properties.correlation_id;

    amqp_channel_t replyChannel = envelope.channel;
    amqp_basic_publish(conn, replyChannel, amqp_empty_bytes, /* routing key*/ envelope.message.properties.reply_to, false, false, &props, amqp_cstring_bytes(comments.c_str()));
    amqp_basic_ack(conn, replyChannel, envelope.delivery_tag, false);

    amqp_destroy_envelope(&envelope);
  }

  redisFree(redisCtxt);
  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  amqp_channel_close(conn, KChannel, AMQP_REPLY_SUCCESS);
  amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  amqp_destroy_connection(conn);

  return 0;
}
