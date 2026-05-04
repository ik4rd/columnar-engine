#pragma once

#if !defined(__SIZEOF_INT128__)
#error "This project requires compiler support for 128-bit integers"
#endif

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpedantic"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif

using Int128 = __int128_t;
using UInt128 = unsigned __int128;

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
