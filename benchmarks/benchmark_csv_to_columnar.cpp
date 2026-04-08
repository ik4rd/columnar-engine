#include <chrono>
#include <exception>
#include <filesystem>
#include <iostream>
#include <string_view>

#include "csv_columnar.h"

#ifndef COLUMNAR_BENCHMARK_DEFAULT_DATA
#define COLUMNAR_BENCHMARK_DEFAULT_DATA "benchmarks/hits_sample.csv"
#endif

#ifndef COLUMNAR_BENCHMARK_DEFAULT_SCHEMA
#define COLUMNAR_BENCHMARK_DEFAULT_SCHEMA "benchmarks/schema_sample.csv"
#endif

#ifndef COLUMNAR_BENCHMARK_DEFAULT_OUTPUT
#define COLUMNAR_BENCHMARK_DEFAULT_OUTPUT "benchmarks/hits_sample.columnar"
#endif

int main(const int argc, char** argv) {
    try {
        size_t rows_per_group = 100000;  // default: 100000
        if (argc == 2) {
            rows_per_group = std::stoull(argv[1]);
        }

        const std::filesystem::path schema_path = COLUMNAR_BENCHMARK_DEFAULT_SCHEMA;
        const std::filesystem::path data_path = COLUMNAR_BENCHMARK_DEFAULT_DATA;
        const std::filesystem::path output_path = COLUMNAR_BENCHMARK_DEFAULT_OUTPUT;

        const auto started_at = std::chrono::steady_clock::now();
        ConvertCsvToColumnar(schema_path, data_path, output_path, rows_per_group);
        const auto finished_at = std::chrono::steady_clock::now();

        const double seconds = std::chrono::duration<double>(finished_at - started_at).count();

        std::cout << "schema: " << schema_path << '\n'
                  << "data: " << data_path << '\n'
                  << "output: " << output_path << '\n'
                  << "rows_per_group: " << rows_per_group << '\n'
                  << "elapsed_seconds: " << seconds << '\n';

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
