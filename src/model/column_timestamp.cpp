#include "model/column_timestamp.h"

#include "common/parsing.h"

void TimestampColumn::AppendFromString(const std::string& value) { AppendValue(ParseTimestamp(value)); }

std::string TimestampColumn::ValueAsString(const size_t row) const { return TimestampToString(ValueAt(row)); }
