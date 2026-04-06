add_library(columnar_engine_lib
        model/batch.cpp
        io/batch_io.cpp
        model/column.cpp
        model/column_int64.cpp
        model/column_string.cpp
        convert/csv_columnar.cpp
        io/csv.cpp
        support/error.cpp
        io/fileio.cpp
        model/metadata.cpp
        support/parsing.cpp
        model/schema.cpp
        io/stream.cpp
)

target_include_directories(columnar_engine_lib PUBLIC ${COLUMNAR_INCLUDE_DIRS})
add_executable(columnar_engine app/main.cpp)

target_link_libraries(columnar_engine PRIVATE columnar_engine_lib)
