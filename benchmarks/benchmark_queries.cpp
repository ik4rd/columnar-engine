#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "executor/executor.h"
#include "io/csv_batch.h"
#include "io/file.h"
#include "support/query_result_checks.h"
#include "support/query_runners.h"

#ifndef COLUMNAR_BENCHMARK_QUERIES_DIR
#define COLUMNAR_BENCHMARK_QUERIES_DIR "benchmarks/queries"
#endif

static constexpr int FirstQueryId = 0;
static constexpr int LastQueryId = 42;

enum class QueryRunStatus {
    Passed,
    Executed,
    Failed,
    Skipped,
    Recorded,
    NoReference,
};

enum class ReferenceCheckMode {
    Auto,
    Always,
    Never,
};

static std::string_view ModeName(const QueryExecutionMode mode) {
    return mode == QueryExecutionMode::Parser ? "parser" : "hardcoded";
}

static std::string_view ReferenceCheckModeName(const ReferenceCheckMode mode) {
    switch (mode) {
        case ReferenceCheckMode::Auto:
            return "auto";
        case ReferenceCheckMode::Always:
            return "always";
        case ReferenceCheckMode::Never:
            return "never";
    }

    return "unknown";
}

static bool LooksLikeSampleInput(const std::filesystem::path& data_path, const std::filesystem::path& queries_dir) {
    const std::filesystem::path sample_path = queries_dir.parent_path() / "hits_sample.columnar";
    std::error_code error;
    if (std::filesystem::equivalent(data_path, sample_path, error)) {
        return true;
    }

    return data_path.filename() == sample_path.filename();
}

QueryRunStatus RunQuery(const Executor& executor, const std::filesystem::path& path, const int query_id,
                        const QueryExecutionMode mode, const PlannedQuery* planned_query, bool record = false,
                        const std::optional<std::filesystem::path>& output_csv = std::nullopt,
                        const bool check_reference = true) {
    const std::string query_sql = std::filesystem::exists(path) ? ReadTextFile(path) : "";
    const bool compare_unordered = QueryUsesGroupBy(query_sql);

    ExecuteExpected result;
    const auto start = std::chrono::steady_clock::now();

    if (mode == QueryExecutionMode::Hardcoded) {
        if (auto fast_result =
                clickbench_fast::TryRun(planned_query != nullptr ? planned_query->table_path : "", query_id);
            fast_result.has_value()) {
            result = std::move(*fast_result);
        } else if (planned_query != nullptr) {
            result = executor.Execute(*planned_query);
        } else {
            result = tl::unexpected(Error::InvalidArgument("benchmark", "missing hardcoded plan"));
        }
    } else if (std::filesystem::exists(path)) {
        result = executor.Execute(query_sql);
    } else {
        std::cout << "query_" << query_id << ": missing, status=skipped" << std::endl;
        return QueryRunStatus::Skipped;
    }

    const auto end = std::chrono::steady_clock::now();
    const auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    if (!result.has_value()) {
        std::cout << "query_" << query_id << ": error=\"" << result.error().GetMessage() << "\", status=failed"
                  << std::endl;
        return QueryRunStatus::Failed;
    }

    Batch result_batch = std::move(*result);
    if (output_csv.has_value()) {
        WriteBatchCsv(*output_csv, result_batch);
    }

    const std::string actual_result = BatchToString(result_batch);
    const std::filesystem::path ref_path =
        std::filesystem::path(COLUMNAR_BENCHMARK_QUERIES_DIR) / ("query_" + std::to_string(query_id) + ".csv");

    std::string status;

    if (record) {
        std::ofstream ref_file(ref_path);
        ref_file << actual_result;
        status = "recorded";
        std::cout << "query_" << query_id << ": rows=" << result_batch.RowsCount() << ", time=" << duration_ms
                  << "ms, status=" << status << std::endl;
        return QueryRunStatus::Recorded;
    } else if (!check_reference) {
        status = "executed";
        std::cout << "query_" << query_id << ": rows=" << result_batch.RowsCount() << ", time=" << duration_ms
                  << "ms, status=" << status << std::endl;
        return QueryRunStatus::Executed;
    } else if (std::filesystem::exists(ref_path)) {
        const std::string expected_result = ReadTextFile(ref_path);
        if (actual_result == expected_result ||
            EqualWithLimitTies(query_sql, result_batch.GetSchema(), actual_result, expected_result) ||
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
            std::cout << "query_" << query_id << ": rows=" << result_batch.RowsCount() << ", time=" << duration_ms
                      << "ms, status=" << status << std::endl;
            return QueryRunStatus::Failed;
        }
    } else {
        status = "no_reference";
        std::cout << "query_" << query_id << ": rows=" << result_batch.RowsCount() << ", time=" << duration_ms
                  << "ms, status=" << status << std::endl;
        return QueryRunStatus::NoReference;
    }

    std::cout << "query_" << query_id << ": rows=" << result_batch.RowsCount() << ", time=" << duration_ms
              << "ms, status=" << status << std::endl;

    return QueryRunStatus::Passed;
}

int main(const int argc, char** argv) {
    try {
        bool record = false;
        bool strict_status = false;
        auto reference_check_mode = ReferenceCheckMode::Auto;

        auto mode = QueryExecutionMode::Hardcoded;

        std::filesystem::path input_path;
        std::optional<std::filesystem::path> output_csv;

        int from_query = FirstQueryId;
        int to_query = LastQueryId;

        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--record") {
                record = true;
            } else if (arg == "--strict-status") {
                strict_status = true;
            } else if (arg == "--check-reference") {
                reference_check_mode = ReferenceCheckMode::Always;
            } else if (arg == "--skip-reference-check") {
                reference_check_mode = ReferenceCheckMode::Never;
            } else if (arg == "--parser") {
                mode = QueryExecutionMode::Parser;
            } else if (arg == "--hardcoded") {
                mode = QueryExecutionMode::Hardcoded;
            } else if (arg == "--mode" && i + 1 < argc) {
                const std::string value = argv[++i];
                if (value == "parser") {
                    mode = QueryExecutionMode::Parser;
                } else if (value == "hardcoded") {
                    mode = QueryExecutionMode::Hardcoded;
                } else {
                    std::cerr << "unknown mode: " << value << " (expected parser or hardcoded)" << std::endl;
                    return 1;
                }
            } else if (arg == "--input" && i + 1 < argc) {
                input_path = std::filesystem::path(argv[++i]);
            } else if (arg == "--output-csv" && i + 1 < argc) {
                output_csv = std::filesystem::path(argv[++i]);
            } else if (arg == "--from" && i + 1 < argc) {
                from_query = std::stoi(argv[++i]);
            } else if (arg == "--to" && i + 1 < argc) {
                to_query = std::stoi(argv[++i]);
            } else {
                std::cerr << "unknown argument: " << arg << std::endl;
                return 1;
            }
        }

        from_query = std::max(from_query, FirstQueryId);
        to_query = std::min(to_query, LastQueryId);

        if (from_query > to_query) {
            std::cerr << "invalid query range" << std::endl;
            return 1;
        }

        if (output_csv.has_value() && from_query != to_query) {
            std::cerr << "--output-csv requires a single query (--from N --to N)" << std::endl;
            return 1;
        }

        const std::filesystem::path queries_dir = COLUMNAR_BENCHMARK_QUERIES_DIR;
        const std::filesystem::path data_path =
            input_path.empty() ? queries_dir.parent_path() / "hits_sample.columnar" : input_path;
        const bool check_reference = reference_check_mode == ReferenceCheckMode::Always
                                         ? true
                                         : reference_check_mode == ReferenceCheckMode::Never
                                               ? false
                                               : LooksLikeSampleInput(data_path, queries_dir);

        Executor executor;
        executor.SetUnsupportedFallbackEnabled(true);
        executor.RegisterTable("hits", data_path);

        std::vector<std::optional<PlannedQuery>> hardcoded_plans(LastQueryId + 1);
        if (mode == QueryExecutionMode::Hardcoded) {
            for (int query_id = FirstQueryId; query_id <= LastQueryId; ++query_id) {
                if (HasHardcodedClickBenchQuery(query_id)) {
                    hardcoded_plans[query_id] = executor.Plan(BuildRequiredClickBenchQuery(query_id));
                }
            }
        }

        size_t found_queries = 0;
        bool saw_failure = false;

        std::cout << "queries_dir=" << queries_dir << std::endl;
        std::cout << "input=" << data_path << std::endl;
        std::cout << "mode=" << ModeName(mode) << std::endl;
        std::cout << "reference_check=" << (check_reference ? "enabled" : "disabled")
                  << " (mode=" << ReferenceCheckModeName(reference_check_mode) << ")" << std::endl;

        for (int query_id = from_query; query_id <= to_query; ++query_id) {
            const std::filesystem::path query_path = queries_dir / ("query_" + std::to_string(query_id) + ".sql");

            if ((mode == QueryExecutionMode::Hardcoded && HasHardcodedClickBenchQuery(query_id)) ||
                std::filesystem::exists(query_path)) {
                ++found_queries;
                const PlannedQuery* planned_query =
                    hardcoded_plans[query_id].has_value() ? &*hardcoded_plans[query_id] : nullptr;
                const QueryRunStatus status =
                    RunQuery(executor, query_path, query_id, mode, planned_query, record, output_csv, check_reference);
                if (status == QueryRunStatus::Failed) {
                    saw_failure = true;
                }
            }
        }

        std::cout << "total_executed=" << found_queries << std::endl;

        if (strict_status && saw_failure) {
            return 2;
        }
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
