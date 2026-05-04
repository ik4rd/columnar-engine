#include "model/column_date.h"

#include "support/parsing.h"

void DateColumn::AppendFromString(const std::string& value) { AppendValue(ParseDate(value)); }

std::string DateColumn::ValueAsString(const size_t row) const { return DateToString(ValueAt(row)); }
