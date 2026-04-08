add_executable(benchmark_csv_to_columnar
        benchmark_csv_to_columnar.cpp
)

target_link_libraries(benchmark_csv_to_columnar
        PRIVATE
        columnar_engine_lib
)

target_compile_definitions(benchmark_csv_to_columnar
        PRIVATE
        COLUMNAR_BENCHMARK_DEFAULT_DATA="${PROJECT_SOURCE_DIR}/benchmarks/hits_sample.csv"
        COLUMNAR_BENCHMARK_DEFAULT_SCHEMA="${PROJECT_SOURCE_DIR}/benchmarks/schema_sample.csv"
        COLUMNAR_BENCHMARK_DEFAULT_OUTPUT="${PROJECT_SOURCE_DIR}/benchmarks/hits_sample.columnar"
)

set_target_properties(benchmark_csv_to_columnar PROPERTIES FOLDER "benchmarks")
