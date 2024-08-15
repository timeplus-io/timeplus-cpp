ADD_EXECUTABLE (insert-test
    main.cpp
)

TARGET_LINK_LIBRARIES (insert-test
    timeplus-cpp-lib
)

IF (MSVC)
    TARGET_LINK_LIBRARIES (insert-test Crypt32)
ENDIF()
