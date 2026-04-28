#include <chrono>
#include <exception>
#include <filesystem>
#include <iostream>
#include <vector>

#include "csv.h"
#include "csv_columnar.h"
#include "error.h"

#ifndef COLUMNAR_BENCHMARK_DEFAULT_DATA
#define COLUMNAR_BENCHMARK_DEFAULT_DATA "benchmarks/hits_sample.csv"
#endif

#ifndef COLUMNAR_BENCHMARK_DEFAULT_SCHEMA
#define COLUMNAR_BENCHMARK_DEFAULT_SCHEMA "benchmarks/schema_sample.csv"
#endif

#ifndef COLUMNAR_BENCHMARK_DEFAULT_OUTPUT
#define COLUMNAR_BENCHMARK_DEFAULT_OUTPUT "benchmarks/hits_sample.columnar"
#endif

#ifndef COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_DATA
#define COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_DATA "benchmarks/hits_sample_new.csv"
#endif

#ifndef COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_SCHEMA
#define COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_SCHEMA "benchmarks/schema_sample_new.csv"
#endif

static constexpr size_t kRowsPerGroup = 50000;

struct CsvCompareResult {
    bool equal = true;
    size_t row = 0;
};

static CsvCompareResult CompareCsvFiles(const std::filesystem::path& lhs_path, const std::filesystem::path& rhs_path) {
    CsvReader lhs(lhs_path);
    CsvReader rhs(rhs_path);

    std::vector<std::string> lhs_row;
    std::vector<std::string> rhs_row;
    size_t row = 0;

    while (true) {
        const bool lhs_has_row = lhs.ReadRow(lhs_row);
        const bool rhs_has_row = rhs.ReadRow(rhs_row);

        if (lhs_has_row != rhs_has_row) {
            return CsvCompareResult{false, row};
        }
        if (!lhs_has_row) {
            return CsvCompareResult{true, row};
        }
        if (lhs_row != rhs_row) {
            return CsvCompareResult{false, row};
        }

        ++row;
    }
}

int main(const int argc, char** argv) {
    try {
        size_t rows_per_group = kRowsPerGroup;
        if (argc == 2) {
            rows_per_group = std::stoull(argv[1]);
        }

        const std::filesystem::path schema_path = COLUMNAR_BENCHMARK_DEFAULT_SCHEMA;
        const std::filesystem::path data_path = COLUMNAR_BENCHMARK_DEFAULT_DATA;
        const std::filesystem::path output_path = COLUMNAR_BENCHMARK_DEFAULT_OUTPUT;
        const std::filesystem::path roundtrip_schema_path = COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_SCHEMA;
        const std::filesystem::path roundtrip_data_path = COLUMNAR_BENCHMARK_DEFAULT_ROUNDTRIP_DATA;

        const auto csv_to_columnar_started_at = std::chrono::steady_clock::now();
        ConvertCsvToColumnar(schema_path, data_path, output_path, rows_per_group);
        const auto csv_to_columnar_finished_at = std::chrono::steady_clock::now();

        const auto columnar_to_csv_started_at = std::chrono::steady_clock::now();
        ConvertColumnarToCsv(output_path, roundtrip_schema_path, roundtrip_data_path);
        const auto columnar_to_csv_finished_at = std::chrono::steady_clock::now();

        const auto compare_started_at = std::chrono::steady_clock::now();
        const CsvCompareResult schema_compare = CompareCsvFiles(schema_path, roundtrip_schema_path);
        const CsvCompareResult data_compare = CompareCsvFiles(data_path, roundtrip_data_path);
        const auto compare_finished_at = std::chrono::steady_clock::now();

        const double csv_to_columnar_seconds =
            std::chrono::duration<double>(csv_to_columnar_finished_at - csv_to_columnar_started_at).count();
        const double columnar_to_csv_seconds =
            std::chrono::duration<double>(columnar_to_csv_finished_at - columnar_to_csv_started_at).count();
        const double compare_seconds = std::chrono::duration<double>(compare_finished_at - compare_started_at).count();

        std::cout << "schema: " << schema_path << std::endl
                  << "data: " << data_path << std::endl
                  << "columnar_output: " << output_path << std::endl
                  << "roundtrip_schema: " << roundtrip_schema_path << std::endl
                  << "roundtrip_data: " << roundtrip_data_path << std::endl
                  << "rows_per_group: " << rows_per_group << std::endl
                  << "csv_to_columnar_elapsed_seconds: " << csv_to_columnar_seconds << std::endl
                  << "columnar_to_csv_elapsed_seconds: " << columnar_to_csv_seconds << std::endl
                  << "csv_compare_elapsed_seconds: " << compare_seconds << std::endl
                  << "schema_matches_source: " << (schema_compare.equal ? "true" : "false") << std::endl
                  << "data_matches_source: " << (data_compare.equal ? "true" : "false") << std::endl;

        if (!schema_compare.equal) {
            std::cout << "schema_first_mismatch_row: " << schema_compare.row << std::endl;
        }
        if (!data_compare.equal) {
            std::cout << "data_first_mismatch_row: " << data_compare.row << std::endl;
        }

        return schema_compare.equal && data_compare.equal ? 0 : 2;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
