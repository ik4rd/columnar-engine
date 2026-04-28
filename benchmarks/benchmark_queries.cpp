#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

#ifndef COLUMNAR_BENCHMARK_QUERIES_DIR
#define COLUMNAR_BENCHMARK_QUERIES_DIR "benchmarks/queries"
#endif

static constexpr int kFirstQueryId = 0;
static constexpr int kLastQueryId = 42;

std::string ReadTextFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("failed to open query file: " + path.string());
    }
    return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

void RunQuerySkeleton(const std::filesystem::path& path, const int query_id) {
    const std::string sql = ReadTextFile(path);
    std::cout << "query_" << query_id << ": found, bytes=" << sql.size() << ", status=skeleton_only" << std::endl;
}

int main() {
    try {
        const std::filesystem::path queries_dir = COLUMNAR_BENCHMARK_QUERIES_DIR;
        size_t found_queries = 0;
        size_t missing_queries = 0;

        std::cout << "queries_dir=" << queries_dir << std::endl;

        for (int query_id = kFirstQueryId; query_id <= kLastQueryId; ++query_id) {
            const std::filesystem::path query_path = queries_dir / ("query_" + std::to_string(query_id) + ".sql");
            if (!std::filesystem::exists(query_path)) {
                ++missing_queries;
                std::cout << "query_" << query_id << ": missing, status=skipped" << std::endl;
                continue;
            }

            ++found_queries;
            RunQuerySkeleton(query_path, query_id);
        }

        std::cout << "found_queries=" << found_queries << std::endl;
        std::cout << "missing_queries=" << missing_queries << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
