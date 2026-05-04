#pragma once

#include <string>
#include <string_view>

#include "executor/executor.h"

size_t FindColumnIndex(const Schema& schema, std::string_view column_name);
void ValidateColumnQualifier(const ParsedQuery& parsed, const ColumnRef& column);
std::string NormalizeLiteral(const ParsedLiteral& literal, ColumnType type);
bool SameColumnName(std::string_view lhs, std::string_view rhs);
bool SameColumnRef(const ColumnRef& lhs, const ColumnRef& rhs);
bool SameSelectItem(const SelectItemSpec& lhs, const SelectItemSpec& rhs);
bool SameOutputName(const SelectItemSpec& lhs, const SelectItemSpec& rhs);
