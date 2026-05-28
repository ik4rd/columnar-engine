#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "executor/query.h"
#include "model/schema.h"

bool IsNumericColumnType(ColumnType type);
Int128 ParseColumnValueAsInt128(ColumnType type, std::string_view value);
std::optional<Int128> TryParseLiteralValueAsInt128(const QueryLiteral& literal, ColumnType type);
std::string FormatInt128Value(ColumnType type, Int128 value);
ComparisonKind FlipComparison(ComparisonKind comparison);
