from tornado.web import Application, RequestHandler
import asyncio
import aio_pika
import uuid
import sys
import aioredis
import time
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool

COMMENTS_REGEX = r'/comments/?'
COMMENTS_VIDEO_REGEX = r'/comments/(?P<id>[a-zA-Z0-9-]+)/?'

async def main():
    loop = asyncio.get_event_loop()

    #setup amqp connections pool
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    amqp_connection_pool: Pool = Pool(get_connection, max_size=10, loop=loop)

    #setup amqp channels pool
    async def get_channel() -> aio_pika.Channel:
        async with amqp_connection_pool.acquire() as connection:
            return await connection.channel()
    amqp_channel_pool: Pool = Pool(get_channel, max_size=10, loop=loop)

    #setup redis connections pool
    redis_pool = aioredis.ConnectionPool.from_url("redis://localhost", max_connections=10)

    async with amqp_connection_pool, amqp_channel_pool:
      urls = [(COMMENTS_VIDEO_REGEX, CommentsByVideoHandler)]
      app = Application(urls, amqp_channel_pool=amqp_channel_pool, redis_pool=redis_pool)
      app.listen(3000)
      await asyncio.Event().wait()
    


class CommentsByVideoHandler(RequestHandler):
  async def sendRequest(self, id):
    amqp_channel_pool = self.application.settings["amqp_channel_pool"]

    async with amqp_channel_pool.acquire() as channel:  # type: aio_pika.Channel
      self.callback_queue = await channel.declare_queue(exclusive=True)
      self.corr_id = str(uuid.uuid4())
      self.future = asyncio.get_running_loop().create_future()

      await channel.default_exchange.publish(
        message=aio_pika.Message(body=bytes(id, encoding='utf-8'), reply_to=self.callback_queue.name, correlation_id=self.corr_id),
        routing_key='task_queue')
              
      await self.callback_queue.consume(self.on_reply)

  async def get(self, id):
    redis_pool = self.application.settings["redis_pool"]
    async with aioredis.Redis(connection_pool=redis_pool) as redis:
      response = await redis.execute_command('get', "comments_" + id)
    if not response:
      await self.sendRequest(id)
      response = await self.future
    await self.finish(response)
    
  async def on_reply(self, message: aio_pika.abc.AbstractIncomingMessage):
    if  message.correlation_id == self.corr_id:
      self.future.set_result(message.body)



if __name__ == '__main__':
  
  try:
    asyncio.run(main())
  except KeyboardInterrupt:
    pass
  finally:
    loop = asyncio.new_event_loop()
    loop.stop()
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()

