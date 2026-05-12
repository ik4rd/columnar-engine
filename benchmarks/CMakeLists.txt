add_executable(benchmark_csv_to_columnar
        benchmark_csv_to_columnar.cpp
)

target_link_libraries(benchmark_csv_to_columnar
        PRIVATE
        columnar_engine_lib
)

add_executable(benchmark_queries
        benchmark_queries.cpp
        support/query_result_checks.cpp
        support/query_result_checks.h
        support/query_runners.h
)

target_link_libraries(benchmark_queries
        PRIVATE
        columnar_engine_lib
)

target_compile_definitions(benchmark_csv_to_columnar
        PRIVATE
        COLUMNAR_BENCHMARK_DEFAULT_DATA="${PROJECT_SOURCE_DIR}/benchmarks/hits_sample.csv"
        COLUMNAR_BENCHMARK_DEFAULT_SCHEMA="${PROJECT_SOURCE_DIR}/benchmarks/scheme.csv"
        COLUMNAR_BENCHMARK_DEFAULT_OUTPUT="${PROJECT_SOURCE_DIR}/benchmarks/hits_sample.columnar"
        COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_DATA="${PROJECT_SOURCE_DIR}/benchmarks/hits_sample_new.csv"
        COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_SCHEMA="${PROJECT_SOURCE_DIR}/benchmarks/schema_sample_new.csv"
)

target_compile_definitions(benchmark_queries
        PRIVATE
        COLUMNAR_BENCHMARK_QUERIES_DIR="${PROJECT_SOURCE_DIR}/benchmarks/queries"
)

set_target_properties(benchmark_csv_to_columnar PROPERTIES FOLDER "benchmarks")
set_target_properties(benchmark_queries PROPERTIES FOLDER "benchmarks")
