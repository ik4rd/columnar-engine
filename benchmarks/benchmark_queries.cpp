#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "csv_batch_io.h"
#include "executor.h"
#include "fileio.h"
#include "query_runners.h"

#ifndef COLUMNAR_BENCHMARK_QUERIES_DIR
#define COLUMNAR_BENCHMARK_QUERIES_DIR "benchmarks/queries"
#endif

static constexpr int FirstQueryId = 0;
static constexpr int LastQueryId = 42;

std::string BatchToString(const Batch& batch) {
    std::vector<std::vector<std::string>> rows;
    AppendBatchRows(batch, rows);

    std::string result;

    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            result += row[i];
            if (i + 1 < row.size()) {
                result += ",";
            }
        }
        result += "\n";
    }

    return result;
}

void RunQuery(const Executor& executor, const std::filesystem::path& path, const int query_id, bool record = false) {
    const auto& runners = GetQueryRunners();
    const auto it = runners.find(query_id);

    ExecuteExpected result;
    const auto start = std::chrono::steady_clock::now();

    if (it != runners.end()) {
        result = it->second(executor);
    } else if (std::filesystem::exists(path)) {
        const std::string sql = ReadTextFile(path);
        result = executor.Execute(sql);
    } else {
        std::cout << "query_" << query_id << ": missing, status=skipped" << std::endl;
        return;
    }

    const auto end = std::chrono::steady_clock::now();
    const auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    if (!result.has_value()) {
        std::cout << "query_" << query_id << ": error=\"" << result.error().GetMessage() << "\", status=failed"
                  << std::endl;
        return;
    }

    const std::string actual_result = BatchToString(*result);
    const std::filesystem::path ref_path =
        std::filesystem::path(COLUMNAR_BENCHMARK_QUERIES_DIR) / ("query_" + std::to_string(query_id) + ".csv");

    std::string status;

    if (record) {
        std::ofstream ref_file(ref_path);
        ref_file << actual_result;
        status = "recorded";
    } else if (std::filesystem::exists(ref_path)) {
        const std::string expected_result = ReadTextFile(ref_path);
        if (actual_result == expected_result) {
            status = "passed";
        } else {
            status = "failed";
            std::cout << "query_" << query_id << ": expected:\n" << expected_result;
            if (!expected_result.empty() && expected_result.back() != '\n') {
                std::cout << std::endl;
            }
            std::cout << "query_" << query_id << ": actual:\n" << actual_result;
            if (!actual_result.empty() && actual_result.back() != '\n') {
                std::cout << std::endl;
            }
        }
    } else {
        status = "no_reference";
    }

    std::cout << "query_" << query_id << ": rows=" << result->RowsCount() << ", time=" << duration_ms
              << "ms, status=" << status << std::endl;
}

int main(const int argc, char** argv) {
    try {
        bool record = false;
        for (int i = 1; i < argc; ++i) {
            if (std::string(argv[i]) == "--record") {
                record = true;
            }
        }

        const std::filesystem::path queries_dir = COLUMNAR_BENCHMARK_QUERIES_DIR;

        Executor executor;
        executor.RegisterTable("hits", "hits_sample.columnar");

        size_t found_queries = 0;

        std::cout << "queries_dir=" << queries_dir << std::endl;

        for (int query_id = FirstQueryId; query_id <= LastQueryId; ++query_id) {
            const std::filesystem::path query_path = queries_dir / ("query_" + std::to_string(query_id) + ".sql");

            if (GetQueryRunners().contains(query_id) || std::filesystem::exists(query_path)) {
                ++found_queries;
                RunQuery(executor, query_path, query_id, record);
            }
        }

        std::cout << "total_executed=" << found_queries << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
