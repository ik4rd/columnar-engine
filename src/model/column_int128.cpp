#include "column_int128.h"

#include "parsing.h"

void Int128Column::AppendFromString(const std::string& value) { AppendValue(ParseInt128(value)); }

std::string Int128Column::ValueAsString(const size_t row) const { return Int128ToString(ValueAt(row)); }
