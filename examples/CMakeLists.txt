add_executable(timeplus-client main.cpp)
target_link_libraries(timeplus-client PRIVATE timeplus-cpp-lib)

add_executable(example-inserter example_inserter.cpp)
target_link_libraries(example-inserter PRIVATE timeplus-cpp-lib)
