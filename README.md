# demo_tornado_api
Simple scalable REST API using tornado and asyncio in Python to handle REST requests,  RabbitMQ as broker, C++ workers to process requests, Cassandra as DB and Redis cache

How to build:
---------------------
Open VS Code from project root folder and build via CMake

How to run:
---------------------
1. Run Cassandra, RabbitMQ and Redis:
sudo systemctl start cassandra
sudo systemctl start rabbitmq-server
redis-server /etc/redis/6379.conf

2. Import data into Cassandra database:
cqlsh -f ./comments.cql

3. Run Tornado with REST service
python3.7 rest/app.py

4. Run one or more C++ workers
./build/worker

5. In browser navigate to :
http://localhost:3000/comments/1 or
http://localhost:3000/comments/2


