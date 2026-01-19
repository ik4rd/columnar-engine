add_library(columnar_engine_lib
        # .cpp files
        batch.cpp
        batch_io.cpp
        column.cpp
        column_int64.cpp
        column_string.cpp
        columnar.cpp
        csv.cpp
        error.cpp
        fileio.cpp
        magic.cpp
        metadata.cpp
        parsing.cpp
        row_group.cpp
        stream.cpp
        utils.cpp
)

target_include_directories(columnar_engine_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
add_executable(columnar_engine main.cpp)

target_link_libraries(columnar_engine PRIVATE columnar_engine_lib)
