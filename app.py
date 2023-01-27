from tornado.web import Application, RequestHandler
import asyncio
import aio_pika
import uuid
import sys
import aioredis
import time

COMMENTS_REGEX = r'/comments/?'
COMMENTS_VIDEO_REGEX = r'/comments/(?P<id>[a-zA-Z0-9-]+)/?'

async def main():
    app = make_app()
    app.listen(3000)

    await asyncio.Event().wait()


class CommentsByVideoHandler(RequestHandler):
  async def sendRequest(self, id):
    self.connection = await aio_pika.connect("amqp://guest:guest@localhost/")
    self.channel = await self.connection.channel()
    self.callback_queue = await self.channel.declare_queue(exclusive=True)

    self.corr_id = str(uuid.uuid4())
    self.future = asyncio.get_running_loop().create_future()

    await self.channel.default_exchange.publish(
      message=aio_pika.Message(body=bytes(id, encoding='utf-8'), reply_to=self.callback_queue.name, correlation_id=self.corr_id),
      routing_key='task_queue')
            
    await self.callback_queue.consume(self.on_reply)

  async def get(self, id):
    start = time.time()
    redis = aioredis.from_url("redis://localhost")
    response = await redis.get("comments_" + id)
    if not response:
      await self.sendRequest(id)
      response = await self.future
    end = time.time()
    print('Request time : ' + str(end-start))
    await self.finish(response)
    
  async def on_reply(self, message: aio_pika.abc.AbstractIncomingMessage):
    if  message.correlation_id == self.corr_id:
      self.future.set_result(message.body)

def make_app():
  urls = [(COMMENTS_VIDEO_REGEX, CommentsByVideoHandler)]
  return Application(urls)
  
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

