ADD_EXECUTABLE (CI-test
    ../../ut/utils.cpp
    main.cpp
    TestFunctions.cpp
)

TARGET_LINK_LIBRARIES (CI-test
    timeplus-cpp-lib
)

IF (MSVC)
    TARGET_LINK_LIBRARIES (CI-test Crypt32)
ENDIF()
