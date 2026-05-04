#include "model/column_string.h"

void StringColumn::AppendFromString(const std::string& value) { AppendValue(value); }

std::string StringColumn::ValueAsString(const size_t row) const { return ValueAt(row); }
