ADD_EXECUTABLE (simple-test
    ../../ut/utils.cpp
    main.cpp
    TestFunctions.cpp
)

TARGET_LINK_LIBRARIES (simple-test
    timeplus-cpp-lib
)

IF (MSVC)
    TARGET_LINK_LIBRARIES (simple-test Crypt32)
ENDIF()
