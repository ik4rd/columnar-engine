#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "executor/executor.h"
#include "io/file.h"
#include "support/query_result_checks.h"
#include "support/query_runners.h"

#ifndef COLUMNAR_BENCHMARK_QUERIES_DIR
#define COLUMNAR_BENCHMARK_QUERIES_DIR "benchmarks/queries"
#endif

static constexpr int FirstQueryId = 0;
static constexpr int LastQueryId = 42;

void RunQuery(const Executor& executor, const std::filesystem::path& path, const int query_id, bool record = false) {
    const auto& runners = GetQueryRunners();
    const auto it = runners.find(query_id);
    const std::string query_sql = std::filesystem::exists(path) ? ReadTextFile(path) : "";
    const bool compare_unordered = QueryUsesGroupBy(query_sql);

    ExecuteExpected result;
    const auto start = std::chrono::steady_clock::now();

    if (it != runners.end()) {
        result = it->second(executor);
    } else if (std::filesystem::exists(path)) {
        result = executor.Execute(query_sql);
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

    Batch result_batch = std::move(*result);
    const std::string actual_result = BatchToString(result_batch);
    const std::filesystem::path ref_path =
        std::filesystem::path(COLUMNAR_BENCHMARK_QUERIES_DIR) / ("query_" + std::to_string(query_id) + ".csv");

    std::string status;

    if (record) {
        std::ofstream ref_file(ref_path);
        ref_file << actual_result;
        status = "recorded";
    } else if (std::filesystem::exists(ref_path)) {
        const std::string expected_result = ReadTextFile(ref_path);
        if (actual_result == expected_result || EqualWithLimitTies(query_sql, result_batch.GetSchema(), actual_result, expected_result) ||
            (compare_unordered && EqualAsRowMultisets(actual_result, expected_result))) {
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

    std::cout << "query_" << query_id << ": rows=" << result_batch.RowsCount() << ", time=" << duration_ms
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
        const std::filesystem::path data_path = queries_dir.parent_path() / "hits_sample.columnar";

        Executor executor;
        executor.SetUnsupportedFallbackEnabled(true);
        executor.RegisterTable("hits", data_path);

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
