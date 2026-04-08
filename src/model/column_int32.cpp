#include "column_int32.h"

#include "parsing.h"

void Int32Column::AppendFromString(const std::string& value) { AppendValue(ParseInt32(value)); }

std::string Int32Column::ValueAsString(const size_t row) const { return std::to_string(ValueAt(row)); }
