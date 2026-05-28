#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "executor/query.h"
#include "model/schema.h"

size_t FindColumnIndex(const Schema& schema, std::string_view column_name);
void ValidateColumnQualifier(const Query& query, const ColumnRef& column);
std::string UnescapeSqlString(std::string_view text);
std::string NormalizeLiteral(const QueryLiteral& literal, ColumnType type);
std::string NormalizeLiteralForEval(const QueryLiteral& literal);
void NormalizeRegexReplacement(std::string& replacement);

bool SameColumnName(std::string_view lhs, std::string_view rhs);
bool SameColumnRef(const ColumnRef& lhs, const ColumnRef& rhs);
bool SameSelectItem(const SelectItemSpec& lhs, const SelectItemSpec& rhs);
bool SameOutputName(const SelectItemSpec& lhs, const SelectItemSpec& rhs);
bool SameExpr(const ExprPtr& lhs, const ExprPtr& rhs);

std::vector<size_t> StarColumnIndexes(const Schema& schema);
std::optional<size_t> TryFindBatchColumn(const Schema& schema, std::string_view name);
