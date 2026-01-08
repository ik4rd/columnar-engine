cmake_minimum_required(VERSION 3.25)

project(columnar-engine)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(cmake/dependencies_package.cmake)
include_directories(${PROJECT_SOURCE_DIR}/include)

add_compile_options(-Wall -Wextra -Werror -Wpedantic)

add_subdirectory(src)
add_subdirectory(examples)

option(ENABLE_TESTS "Build tests" ON)
if (ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif ()