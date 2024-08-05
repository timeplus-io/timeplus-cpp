add_executable(timeplus-client 
    ../ut/utils.cpp
    main.cpp)

target_link_libraries(timeplus-client PRIVATE timeplus-cpp-lib)
