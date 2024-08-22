# ADD_EXECUTABLE (bench
#     bench.cpp
# )

# TARGET_LINK_LIBRARIES (bench
#     timeplus-cpp-lib
#     benchmark
# )

add_executable(insert-sync-benchmark insert_sync_benchmark.cpp)
target_link_libraries(insert-sync-benchmark timeplus-cpp-lib)

add_executable(insert-async-benchmark insert_async_benchmark.cpp)
target_link_libraries(insert-async-benchmark timeplus-cpp-lib)
