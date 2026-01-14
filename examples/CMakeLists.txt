function(add_component_example name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE columnar_engine_lib)
    target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include)
endfunction()

add_component_example(example_fileio example_fileio.cpp)
add_component_example(example_csv example_csv.cpp)
add_component_example(example_columnar example_columnar.cpp)
