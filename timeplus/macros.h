#pragma once

#if defined(NDEBUG) || !defined(TRACE_TIMEPLUS_CPP)
#define TRACE(format, ...)
#else
#define TRACE(format, ...)           \
    do {                             \
        printf("[trace] ");          \
        printf(format, __VA_ARGS__); \
        printf("\n");                \
    } while (0)
#endif
