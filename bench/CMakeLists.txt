ADD_EXECUTABLE (bench
    bench.cpp
)

TARGET_LINK_LIBRARIES (bench
    timeplus-cpp-lib
    benchmark
)
