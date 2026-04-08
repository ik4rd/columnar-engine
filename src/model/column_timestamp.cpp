#include "column_timestamp.h"

#include "parsing.h"

void TimestampColumn::AppendFromString(const std::string& value) { AppendValue(ParseTimestamp(value)); }

std::string TimestampColumn::ValueAsString(const size_t row) const { return TimestampToString(ValueAt(row)); }
