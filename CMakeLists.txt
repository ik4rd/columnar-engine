cmake_minimum_required(VERSION 3.25)

project(columnar-engine)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(FETCHCONTENT_BASE_DIR "${CMAKE_BINARY_DIR}/.deps")

option(ENABLE_TESTS "Build tests" ON)

include(cmake/dependencies_package.cmake)
include(cmake/sanitizers.cmake)

set(COLUMNAR_INCLUDE_DIRS
        ${PROJECT_SOURCE_DIR}/include/convert
        ${PROJECT_SOURCE_DIR}/include/io
        ${PROJECT_SOURCE_DIR}/include/model
        ${PROJECT_SOURCE_DIR}/include/support
        ${PROJECT_SOURCE_DIR}/include/testing
)

include_directories(${COLUMNAR_INCLUDE_DIRS})

add_compile_options(-Wall -Wextra -Werror -Wpedantic)

set(COLUMNAR_SANITIZERS "" CACHE STRING "Comma or semicolon separated list of sanitizers: address,undefined,leak,thread,memory")
columnar_enable_sanitizers("${COLUMNAR_SANITIZERS}")

set(COLUMNAR_SANITIZER_TARGET_BUILD_DIR "${PROJECT_SOURCE_DIR}/build-sanitizers" CACHE PATH
        "Build directory used by the test_sanitizers target")
set(COLUMNAR_SANITIZER_TARGET_SANITIZERS "address,undefined" CACHE STRING
        "Sanitizers used by the test_sanitizers target")

add_subdirectory(src)
add_subdirectory(examples)

if (ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif ()

set(columnar_sanitizer_configure_args
        -S ${PROJECT_SOURCE_DIR}
        -B ${COLUMNAR_SANITIZER_TARGET_BUILD_DIR}
        -G ${CMAKE_GENERATOR}
        -DENABLE_TESTS=ON
        -DCOLUMNAR_SANITIZERS=${COLUMNAR_SANITIZER_TARGET_SANITIZERS}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
)

if (CMAKE_GENERATOR_PLATFORM)
    list(APPEND columnar_sanitizer_configure_args -A ${CMAKE_GENERATOR_PLATFORM})
endif()

if (CMAKE_GENERATOR_TOOLSET)
    list(APPEND columnar_sanitizer_configure_args -T ${CMAKE_GENERATOR_TOOLSET})
endif()

if (CMAKE_CONFIGURATION_TYPES)
    add_custom_target(test_sanitizers
            COMMAND ${CMAKE_COMMAND} ${columnar_sanitizer_configure_args}
            COMMAND ${CMAKE_COMMAND} --build ${COLUMNAR_SANITIZER_TARGET_BUILD_DIR} --config Debug
            COMMAND ${CMAKE_CTEST_COMMAND} --test-dir ${COLUMNAR_SANITIZER_TARGET_BUILD_DIR} -C Debug --output-on-failure
            USES_TERMINAL
            COMMENT "Configure, build and run tests with sanitizers"
    )
else()
    add_custom_target(test_sanitizers
            COMMAND ${CMAKE_COMMAND} ${columnar_sanitizer_configure_args} -DCMAKE_BUILD_TYPE=Debug
            COMMAND ${CMAKE_COMMAND} --build ${COLUMNAR_SANITIZER_TARGET_BUILD_DIR}
            COMMAND ${CMAKE_CTEST_COMMAND} --test-dir ${COLUMNAR_SANITIZER_TARGET_BUILD_DIR} --output-on-failure
            USES_TERMINAL
            COMMENT "Configure, build and run tests with sanitizers"
    )
endif()
