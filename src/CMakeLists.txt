add_library(columnar_engine_lib
        # .cpp files
        error.cpp
        fileio.cpp
        columnar.cpp
        csv.cpp
)

target_include_directories(columnar_engine_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
add_executable(columnar_engine main.cpp)

target_link_libraries(columnar_engine PRIVATE columnar_engine_lib)
