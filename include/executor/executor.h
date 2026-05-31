#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/error.h"
#include "executor/query_plan.h"
#include "model/batch.h"
#include "tl/expected.hpp"

struct Query;

using ExecuteExpected = tl::expected<Batch, Error>;

class Executor {
   public:
    Executor() = default;

    void RegisterTable(const std::string& name, std::filesystem::path path);
    void SetUnsupportedFallbackEnabled(bool enabled);

    PlannedQuery Plan(const Query& query) const;
    ExecuteExpected Execute(std::string_view query) const;
    ExecuteExpected Execute(const Query& query) const;
    ExecuteExpected Execute(const PlannedQuery& planned) const;

   private:
    ExecuteExpected ExecutePlanned(const Query& query) const;
    static ExecuteExpected ExecutePlanned(const PlannedQuery& planned);

    std::unordered_map<std::string, std::filesystem::path> tables_;
    bool unsupported_fallback_enabled_ = false;
};
