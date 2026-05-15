add_library(columnar_engine_core
        model/batch.cpp
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
        model/metadata.cpp
        io/batch.cpp
        io/file.cpp
        io/stream.cpp
        support/error.cpp
        support/ascii.cpp
        support/parsing.cpp
)

target_include_directories(columnar_engine_core PUBLIC ${COLUMNAR_INCLUDE_DIRS})

add_library(columnar_engine_columnar
        io/columnar_batch.cpp
)

target_link_libraries(columnar_engine_columnar PUBLIC columnar_engine_core)

add_library(columnar_engine_csv
        io/csv.cpp
        io/csv_batch.cpp
        model/schema_csv.cpp
)

target_link_libraries(columnar_engine_csv PUBLIC columnar_engine_core)

add_library(columnar_engine_convert
        convert/csv_columnar.cpp
)

target_link_libraries(columnar_engine_convert
        PUBLIC
        columnar_engine_columnar
        columnar_engine_csv
)

add_library(columnar_engine_executor
        executor/executor.cpp
        executor/query_parser.cpp
        executor/query_planner.cpp
        executor/planner_utils.cpp
        executor/aggregate_states.cpp
        executor/operator_utils.cpp
        executor/operators.cpp
        sql_parser/tokenizer_factory.cpp
        sql_parser/tokenizer_tokens.cpp
        sql_parser/tokenizer.cpp
)

target_link_libraries(columnar_engine_executor PUBLIC columnar_engine_columnar)

add_library(columnar_engine_lib INTERFACE)

target_link_libraries(columnar_engine_lib
        INTERFACE
        columnar_engine_core
        columnar_engine_columnar
        columnar_engine_csv
        columnar_engine_convert
        columnar_engine_executor
)

add_executable(columnar_engine app/main.cpp)
target_link_libraries(columnar_engine PRIVATE columnar_engine_lib argparse)
