project(worker)
set(CMAKE_CXX_STANDARD 11)

add_executable(worker worker.cpp utils.cpp)
target_link_libraries(worker rabbitmq cassandra uv hiredis)
