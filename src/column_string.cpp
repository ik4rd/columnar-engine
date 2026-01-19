#include "column_string.h"

#include <limits>
#include <utility>

#include "stream.h"
#include "utils.h"

StringColumn::StringColumn() : Column(ColumnType::String) {}

size_t StringColumn::Size() const { return values_.size(); }

void StringColumn::Reserve(const size_t n) { values_.reserve(n); }

void StringColumn::Clear() { values_.clear(); }

void StringColumn::AppendFromString(const std::string& value) { values_.push_back(value); }

std::string StringColumn::ValueAsString(const size_t row) const {
    CheckIndex("column_string", row, values_.size());
    return values_[row];
}

void StringColumn::WriteTo(std::ostream& out) const {
    for (const std::string& value : values_) {
        if (value.size() > std::numeric_limits<uint32_t>::max()) {
            throw error::MakeError("column_string", "string value exceeds supported size");
        }
        WriteLittleEndian<uint32_t>(out, static_cast<uint32_t>(value.size()));
        WriteBytes(out, value);
    }
}

void StringColumn::ReadFrom(std::istream& in, const uint32_t row_count, const uint64_t size) {
    values_.clear();
    values_.reserve(row_count);

    uint64_t consumed = 0;
    for (uint32_t row_index = 0; row_index < row_count; ++row_index) {
        const uint32_t length = ReadLittleEndian<uint32_t>(in);
        consumed += sizeof(uint32_t);

        std::string value(length, '\0');
        ReadBytes(in, value.data(), value.size());
        consumed += length;
        values_.push_back(std::move(value));
    }

    if (consumed != size) {
        throw error::MakeError("column_string", "string column chunk size mismatch");
    }
}
