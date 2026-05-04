#include "model/column_int16.h"

#include "support/parsing.h"

void Int16Column::AppendFromString(const std::string& value) { AppendValue(ParseInt16(value)); }

std::string Int16Column::ValueAsString(const size_t row) const { return std::to_string(ValueAt(row)); }
