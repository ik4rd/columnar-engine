#include "model/column_int64.h"

#include "support/parsing.h"

void Int64Column::AppendFromString(const std::string& value) { AppendValue(ParseInt64(value)); }

std::string Int64Column::ValueAsString(const size_t row) const { return std::to_string(ValueAt(row)); }
