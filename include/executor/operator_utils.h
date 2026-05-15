#pragma once

#include <string_view>

#include "executor/query_plan.h"

int CompareValues(ColumnType type, std::string_view lhs, std::string_view rhs);
bool MatchesComparison(ColumnType type, std::string_view lhs, std::string_view rhs, ComparisonKind kind);
ColumnType AggregateOutputType(const PlannedAgg& aggregate);
