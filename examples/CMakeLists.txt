add_executable(timeplus-client 
    ../ut/utils.cpp
    main.cpp)

target_link_libraries(timeplus-client PRIVATE timeplus-cpp-lib)

add_executable(insert-examples insert_examples.cpp)
target_link_libraries(insert-examples PRIVATE timeplus-cpp-lib)
