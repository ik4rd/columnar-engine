include(GoogleTest)

function(add_component_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE columnar_engine_lib GTest::gtest_main)
    gtest_discover_tests(${name})
endfunction()

# components
add_component_test(test_fileio test_fileio.cpp)
add_component_test(test_columnar test_columnar.cpp)
add_component_test(test_csv test_csv.cpp)
add_component_test(test_batch test_batch.cpp)
add_component_test(test_columns test_columns.cpp)
