if (ENABLE_TESTS)
    find_package(GTest QUIET)
    if (NOT GTest_FOUND)
        message(WARNING "GTest not found; tests will be disabled for this configure run.")
        set(ENABLE_TESTS OFF CACHE BOOL "Build tests" FORCE)
    endif ()
endif ()

include(FetchContent)
FetchContent_Declare(
        argparse
        GIT_REPOSITORY https://github.com/p-ranav/argparse.git
)
FetchContent_MakeAvailable(argparse)
