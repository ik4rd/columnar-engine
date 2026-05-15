#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>

#include "executor/query.h"
#include "executor/query_plan.h"

PlannedQuery PlanQuery(const Query& query, const std::unordered_map<std::string, std::filesystem::path>& tables);
