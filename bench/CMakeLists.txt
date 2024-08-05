# ADD_EXECUTABLE (bench
#     bench.cpp
# )
#
# TARGET_LINK_LIBRARIES (bench
#     timeplus-cpp-lib
#     benchmark
# )

add_executable(huatai-benchmark huatai_benchmark.cpp)
target_link_libraries(huatai-benchmark timeplus-cpp-lib)
