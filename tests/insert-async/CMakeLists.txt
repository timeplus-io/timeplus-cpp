ADD_EXECUTABLE (insert-async-test
    main.cpp
)

TARGET_LINK_LIBRARIES (insert-async-test
    timeplus-cpp-lib
)

IF (MSVC)
    TARGET_LINK_LIBRARIES (insert-async-test Crypt32)
ENDIF()
