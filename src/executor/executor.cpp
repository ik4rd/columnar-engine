#include "executor/executor.h"

#include <utility>

#include "support/ascii.h"

void Executor::RegisterTable(const std::string& name, std::filesystem::path path) {
    tables_[ToLowerAscii(name)] = std::move(path);
}

ExecuteExpected Executor::Execute(const std::string_view query) const {
    try {
        const ParsedQuery parsed = ParseQuery(query);
        const PlannedQuery planned = PlanQuery(parsed, tables_);

        std::unique_ptr<Operator> root = BuildPlan(planned);

        auto batch = root->Next();
        if (!batch.has_value()) {
            return Batch{};
        }

        return std::move(*batch);
    } catch (const Error& error) {
        return tl::unexpected(error);
    }
}
