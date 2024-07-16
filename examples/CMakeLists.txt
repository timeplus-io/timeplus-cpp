add_executable(timeplusd-client main.cpp)

target_link_libraries(timeplusd-client PRIVATE clickhouse-cpp-lib)
