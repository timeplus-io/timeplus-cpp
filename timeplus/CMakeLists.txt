SET ( timeplus-cpp-lib-src
    base/compressed.cpp
    base/input.cpp
    base/output.cpp
    base/platform.cpp
    base/socket.cpp
    base/wire_format.cpp
    base/endpoints_iterator.cpp

    columns/array.cpp
    columns/column.cpp
    columns/date.cpp
    columns/decimal.cpp
    columns/enum.cpp
    columns/factory.cpp
    columns/geo.cpp
    columns/ip4.cpp
    columns/ip6.cpp
    columns/lowcardinality.cpp
    columns/nullable.cpp
    columns/numeric.cpp
    columns/map.cpp
    columns/string.cpp
    columns/tuple.cpp
    columns/uuid.cpp

    columns/itemview.cpp

    types/type_parser.cpp
    types/types.cpp

    block.cpp
    client.cpp
    client_pool.cpp
    query.cpp
    timeplus.cpp

    # Headers
    base/buffer.h
    base/compressed.h
    base/endpoints_iterator.h
    base/input.h
    base/open_telemetry.h
    base/output.h
    base/platform.h
    base/projected_iterator.h
    base/singleton.h
    base/socket.h
    base/sslsocket.h
    base/string_utils.h
    base/string_view.h
    base/uuid.h
    base/wire_format.h

    columns/array.h
    columns/column.h
    columns/date.h
    columns/decimal.h
    columns/enum.h
    columns/factory.h
    columns/geo.h
    columns/ip4.h
    columns/ip6.h
    columns/itemview.h
    columns/lowcardinality.h
    columns/lowcardinalityadaptor.h
    columns/map.h
    columns/nothing.h
    columns/nullable.h
    columns/numeric.h
    columns/string.h
    columns/tuple.h
    columns/utils.h
    columns/uuid.h

    types/type_parser.h
    types/types.h

    block.h
    blocking_queue.h
    client.h
    client_pool.h
    error_codes.h
    exceptions.h
    protocol.h
    query.h
    server_exception.h
    timeplus.h
    timeplus_config.h
)

if (MSVC)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_compile_options(/W4)
    # remove in 3.0
    add_compile_options(/wd4996)
else()
    set(cxx_extra_wall "-Wempty-body -Wconversion -Wreturn-type -Wparentheses -Wuninitialized -Wunreachable-code -Wunused-function -Wunused-value -Wunused-variable")   
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${cxx_extra_wall}")

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
        # a little abnormal when clang check conversion
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${cxx_extra_wall} -Wno-conversion")
    endif()
endif()

IF (WITH_OPENSSL)
    LIST(APPEND timeplus-cpp-lib-src base/sslsocket.cpp)
ENDIF ()

ADD_LIBRARY (timeplus-cpp-lib ${timeplus-cpp-lib-src}
    version.h)
SET_TARGET_PROPERTIES (timeplus-cpp-lib
    PROPERTIES
    LINKER_LANGUAGE CXX
    VERSION ${TIMEPLUS_CPP_VERSION}
)
TARGET_LINK_LIBRARIES (timeplus-cpp-lib
    absl::int128
    cityhash::cityhash
    lz4::lz4
    zstd::zstd
)
TARGET_INCLUDE_DIRECTORIES (timeplus-cpp-lib
    PUBLIC ${PROJECT_SOURCE_DIR}
)

IF (NOT BUILD_SHARED_LIBS)
    ADD_LIBRARY (timeplus-cpp-lib-static ALIAS timeplus-cpp-lib)
ELSE ()
    SET_TARGET_PROPERTIES (timeplus-cpp-lib
        PROPERTIES
        SO_VERSION ${TIMEPLUS_CPP_VERSION}
        SO_VERSION ${TIMEPLUS_CPP_VERSION_MAJOR}
    )
ENDIF ()


IF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    INCLUDE (CheckCXXSourceCompiles)

    CHECK_CXX_SOURCE_COMPILES("#include <bits/c++config.h>\nint main() { return __GLIBCXX__ != 0; }"
                              BUILDING_WITH_LIB_STDCXX)

    IF (BUILDING_WITH_LIB_STDCXX)
        # there is a problem with __builtin_mul_overflow call at link time
        # the error looks like: ... undefined reference to `__muloti4' ...
        # caused by clang bug https://bugs.llvm.org/show_bug.cgi?id=16404
        # explicit linking to compiler-rt allows to workaround the problem
        SET (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --rtlib=compiler-rt")

        # some workaround for linking issues on linux:
        # /usr/bin/ld: CMakeFiles/simple-test.dir/main.cpp.o: undefined reference to symbol '_Unwind_Resume@@GCC_3.0'
        # /usr/bin/ld: /lib/x86_64-linux-gnu/libgcc_s.so.1: error adding symbols: DSO missing from command line
        # FIXME: that workaround breaks clang build on mingw
        TARGET_LINK_LIBRARIES (timeplus-cpp-lib gcc_s)
    ENDIF ()
ENDIF ()


INSTALL (TARGETS timeplus-cpp-lib
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

# general
INSTALL(FILES block.h DESTINATION include/timeplus/)
INSTALL(FILES blocking_queue.h DESTINATION include/timeplus/)
INSTALL(FILES client.h DESTINATION include/timeplus/)
INSTALL(FILES client_pool.h DESTINATION include/timeplus/)
INSTALL(FILES error_codes.h DESTINATION include/timeplus/)
INSTALL(FILES exceptions.h DESTINATION include/timeplus/)
INSTALL(FILES server_exception.h DESTINATION include/timeplus/)
INSTALL(FILES protocol.h DESTINATION include/timeplus/)
INSTALL(FILES query.h DESTINATION include/timeplus/)
INSTALL(FILES timeplus.h DESTINATION include/timeplus/)
INSTALL(FILES timeplus_config.h DESTINATION include/timeplus/)
INSTALL(FILES version.h DESTINATION include/timeplus/)

# base
INSTALL(FILES base/buffer.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/compressed.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/input.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/open_telemetry.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/output.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/platform.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/projected_iterator.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/scope_guard.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/singleton.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/socket.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/string_utils.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/string_view.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/uuid.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/wire_format.h DESTINATION include/timeplus/base/)
INSTALL(FILES base/endpoints_iterator.h DESTINATION include/timeplus/base/)

# columns
INSTALL(FILES columns/array.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/column.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/date.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/decimal.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/enum.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/factory.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/geo.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/ip4.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/ip6.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/itemview.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/lowcardinality.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/nullable.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/numeric.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/map.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/string.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/tuple.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/utils.h DESTINATION include/timeplus/columns/)
INSTALL(FILES columns/uuid.h DESTINATION include/timeplus/columns/)

# types
INSTALL(FILES types/type_parser.h DESTINATION include/timeplus/types/)
INSTALL(FILES types/types.h DESTINATION include/timeplus/types/)

IF (WITH_OPENSSL)
    TARGET_LINK_LIBRARIES (timeplus-cpp-lib OpenSSL::SSL)
ENDIF ()

IF (WIN32 OR MINGW)
    TARGET_LINK_LIBRARIES (timeplus-cpp-lib wsock32 ws2_32)
ENDIF ()
