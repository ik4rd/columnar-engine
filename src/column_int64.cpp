#include "column_int64.h"

#include "parsing.h"
#include "stream.h"
#include "utils.h"

Int64Column::Int64Column() : Column(ColumnType::Int64) {}

size_t Int64Column::Size() const { return values_.size(); }

void Int64Column::Reserve(const size_t n) { values_.reserve(n); }

void Int64Column::Clear() { values_.clear(); }

void Int64Column::AppendFromString(const std::string& value) { values_.push_back(ParseInt64(value)); }

std::string Int64Column::ValueAsString(const size_t row) const {
    CheckIndex("column_int64", row, values_.size());
    return std::to_string(values_[row]);
}

void Int64Column::WriteTo(std::ostream& out) const {
    for (const int64_t value : values_) {
        WriteLittleEndian<int64_t>(out, value);
    }
}

void Int64Column::ReadFrom(std::istream& in, const uint32_t row_count, const uint64_t size) {
    const uint64_t expected = static_cast<uint64_t>(row_count) * sizeof(int64_t);
    if (size != expected) {
        throw error::MakeError("column_int64", "int64 column chunk size mismatch");
    }

    values_.assign(row_count, 0);
    for (uint32_t row_index = 0; row_index < row_count; ++row_index) {
        values_[row_index] = ReadLittleEndian<int64_t>(in);
    }
}
