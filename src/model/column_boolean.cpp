#include "model/column_boolean.h"

#include "common/parsing.h"

void BooleanColumn::AppendFromString(const std::string& value) { AppendValue(ParseBoolean(value) ? 1 : 0); }

std::string BooleanColumn::ValueAsString(const size_t row) const { return BooleanToString(ValueAt(row) != 0); }
