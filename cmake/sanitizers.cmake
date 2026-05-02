function(columnar_enable_sanitizers sanitizers_raw)
    set(CMAKE_CXX_STANDARD 23)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    
    if (NOT sanitizers_raw)
        return()
    endif ()

    string(REPLACE "," ";" sanitizers "${sanitizers_raw}")
    list(REMOVE_DUPLICATES sanitizers)

    set(supported_sanitizers address undefined leak thread memory)
    foreach (sanitizer IN LISTS sanitizers)
        if (NOT sanitizer IN_LIST supported_sanitizers)
            message(FATAL_ERROR "Unsupported sanitizer: ${sanitizer}")
        endif ()
    endforeach ()

    if ("thread" IN_LIST sanitizers AND "address" IN_LIST sanitizers)
        message(FATAL_ERROR "ThreadSanitizer cannot be combined with AddressSanitizer")
    endif ()

    if ("thread" IN_LIST sanitizers AND "leak" IN_LIST sanitizers)
        message(FATAL_ERROR "ThreadSanitizer cannot be combined with LeakSanitizer")
    endif ()

    if ("memory" IN_LIST sanitizers AND "address" IN_LIST sanitizers)
        message(FATAL_ERROR "MemorySanitizer cannot be combined with AddressSanitizer")
    endif ()

    if ("memory" IN_LIST sanitizers AND "thread" IN_LIST sanitizers)
        message(FATAL_ERROR "MemorySanitizer cannot be combined with ThreadSanitizer")
    endif ()

    if ("memory" IN_LIST sanitizers AND "leak" IN_LIST sanitizers)
        message(FATAL_ERROR "MemorySanitizer cannot be combined with LeakSanitizer")
    endif ()

    if ("memory" IN_LIST sanitizers)
        if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            message(FATAL_ERROR "MemorySanitizer requires Clang")
        endif ()
        if (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
            message(FATAL_ERROR "MemorySanitizer is not supported by AppleClang")
        endif ()
        if (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
            message(FATAL_ERROR "MemorySanitizer is only supported in this project on Linux")
        endif ()
    endif ()

    string(JOIN "," sanitizer_flags ${sanitizers})

    add_compile_options(
            -fno-omit-frame-pointer
            -fno-optimize-sibling-calls
            -fsanitize=${sanitizer_flags}
    )

    add_link_options(-fsanitize=${sanitizer_flags})

    message(STATUS "Enabled sanitizers: ${sanitizer_flags}")
endfunction()
