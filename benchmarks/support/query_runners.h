#pragma once

#include <filesystem>
#include <optional>

#include "executor/executor.h"
#include "executor/query.h"

enum class QueryExecutionMode {
    Hardcoded,
    Parser,
};

namespace clickbench_fast {
std::optional<ExecuteExpected> TryRun(const std::filesystem::path& path, int query_id);
}  // namespace clickbench_fast

bool HasHardcodedClickBenchQuery(int query_id);
Query BuildRequiredClickBenchQuery(int query_id);
ExecuteExpected RunHardcodedClickBenchQuery(const Executor& executor, int query_id);
