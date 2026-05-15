#include "executor/executor.h"

#include <memory>
#include <utility>

#include "executor/operator.h"
#include "executor/query_parser.h"
#include "executor/query_planner.h"
#include "support/ascii.h"

void Executor::RegisterTable(const std::string& name, std::filesystem::path path) {
    tables_[ToLowerAscii(name)] = std::move(path);
}

ExecuteExpected Executor::Execute(const std::string_view query) const {
    try {
        const Query query_ast = ParseQuery(query);
        const PlannedQuery planned = PlanQuery(query_ast, tables_);

        const std::unique_ptr<Operator> root = BuildPlan(planned);

        auto batch = root->Next();
        if (!batch.has_value()) {
            return Batch{};
        }

        return std::move(*batch);
    } catch (const Error& error) {
        return tl::unexpected(error);
    }
}
