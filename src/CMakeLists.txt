add_library(columnar_engine_lib
        model/batch.cpp
        io/batch_io.cpp
        model/column.cpp
        model/column_boolean.cpp
        model/column_character.cpp
        model/column_date.cpp
        model/column_int16.cpp
        model/column_int32.cpp
        model/column_int128.cpp
        model/column_int64.cpp
        model/column_string.cpp
        model/column_timestamp.cpp
        convert/csv_columnar.cpp
        io/csv.cpp
        support/error.cpp
        support/ascii.cpp
        io/fileio.cpp
        model/metadata.cpp
        support/parsing.cpp
        model/schema.cpp
        io/stream.cpp
        executor/executor.cpp
        executor/query_parser.cpp
        executor/query_planner.cpp
        executor/aggregate_states.cpp
        executor/operator_utils.cpp
        executor/operators.cpp
        sql_parser/tokenizer_factory.cpp
        sql_parser/tokenizer_tokens.cpp
        sql_parser/tokenizer.cpp
)

target_include_directories(columnar_engine_lib PUBLIC ${COLUMNAR_INCLUDE_DIRS})
add_executable(columnar_engine app/main.cpp)
target_link_libraries(columnar_engine PRIVATE columnar_engine_lib argparse)
