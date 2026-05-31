#include "executor/executor.h"

#include <memory>
#include <utility>

#include "common/ascii.h"
#include "executor/operator.h"
#include "executor/query_parser.h"
#include "executor/query_planner.h"

void Executor::RegisterTable(const std::string& name, std::filesystem::path path) {
    tables_[ToLowerAscii(name)] = std::move(path);
}

void Executor::SetUnsupportedFallbackEnabled(const bool enabled) { unsupported_fallback_enabled_ = enabled; }

PlannedQuery Executor::Plan(const Query& query) const { return PlanQuery(query, tables_); }

ExecuteExpected Executor::Execute(const std::string_view query) const {
    try {
        return ExecutePlanned(ParseQuery(query));
    } catch (const Error& error) {
        if (unsupported_fallback_enabled_) {
            return Batch{};
        }
        return tl::unexpected(error);
    }
}

ExecuteExpected Executor::Execute(const Query& query) const {
    try {
        return ExecutePlanned(query);
    } catch (const Error& error) {
        if (unsupported_fallback_enabled_) {
            return Batch{};
        }
        return tl::unexpected(error);
    }
}

ExecuteExpected Executor::Execute(const PlannedQuery& planned) const {
    try {
        return ExecutePlanned(planned);
    } catch (const Error& error) {
        if (unsupported_fallback_enabled_) {
            return Batch{};
        }
        return tl::unexpected(error);
    }
}

ExecuteExpected Executor::ExecutePlanned(const Query& query) const { return ExecutePlanned(Plan(query)); }

ExecuteExpected Executor::ExecutePlanned(const PlannedQuery& planned) {
    const std::unique_ptr<Operator> root = BuildPlan(planned);

    auto batch = root->Next();
    if (!batch.has_value()) {
        return Batch{};
    }

    return std::move(batch).value();
}
