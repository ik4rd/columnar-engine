include(GoogleTest)

function(add_component_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE columnar_engine_lib GTest::gtest_main)
    gtest_discover_tests(${name})
endfunction()

# components
add_component_test(test_foo test_foo.cpp)