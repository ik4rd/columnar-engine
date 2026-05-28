find_path(LZ4_INCLUDE_DIR lz4.h)
find_library(LZ4_LIBRARY lz4)

if (NOT LZ4_INCLUDE_DIR OR NOT LZ4_LIBRARY)
    message(FATAL_ERROR "lz4 headers/library not found")
endif ()

add_library(lz4_external INTERFACE)
target_include_directories(lz4_external INTERFACE ${LZ4_INCLUDE_DIR})
target_link_libraries(lz4_external INTERFACE ${LZ4_LIBRARY})

add_library(columnar_engine_core
        model/batch.cpp
        model/column.cpp
        model/column_string.cpp
        model/metadata.cpp
        io/batch.cpp
        io/compression.cpp
        io/file.cpp
        io/stream.cpp
        common/error.cpp
        common/ascii.cpp
        common/parsing.cpp
        common/operator_utils.cpp
        common/planner_utils.cpp
)

target_include_directories(columnar_engine_core PUBLIC ${COLUMNAR_INCLUDE_DIRS})
target_link_libraries(columnar_engine_core PUBLIC lz4_external)

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
        executor/aggregate_states.cpp
        executor/operators.cpp
        executor/operators_aggregate.cpp
        executor/operators_runtime.cpp
        executor/operators_scan.cpp
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
