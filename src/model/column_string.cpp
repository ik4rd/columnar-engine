#include "model/column_string.h"

#include <limits>
#include <memory>
#include <utility>

#include "io/stream.h"
#include "common/error.h"

StringColumn::StringColumn() : MutableColumn(ColumnType::String) {}

size_t StringColumn::Size() const { return values_.size(); }

void StringColumn::Reserve(const size_t n) { values_.reserve(n); }

void StringColumn::Clear() { values_.clear(); }

void StringColumn::AppendFromString(const std::string& value) { values_.push_back(value); }

std::string StringColumn::ValueAsString(const size_t row) const {
    CheckRowIndex(ModuleName(), row, values_.size());
    return values_[row];
}

std::unique_ptr<Column> StringColumn::Clone() const { return std::make_unique<StringColumn>(*this); }

std::unique_ptr<MutableColumn> StringColumn::CloneMutable() const { return std::make_unique<StringColumn>(*this); }

void StringColumn::WriteTo(std::ostream& out) const {
    size_t total_size = 0;

    for (const auto& value : values_) {
        if (value.size() > std::numeric_limits<uint32_t>::max()) {
            throw Error::Overflow(ModuleName(), "value exceeds supported size");
        }
        total_size += sizeof(uint32_t) + value.size();
    }

    std::string buffer(total_size, '\0');
    char* dst = buffer.data();

    for (const auto& value : values_) {
        const uint32_t length = value.size();

        std::memcpy(dst, &length, sizeof(length));
        dst += sizeof(length);

        if (!value.empty()) {
            std::memcpy(dst, value.data(), value.size());
            dst += value.size();
        }
    }

    WriteBytes(out, buffer);
}

void StringColumn::ReadFrom(std::istream& in, const uint32_t row_count, const uint64_t size) {
    values_.clear();
    values_.reserve(row_count);

    uint64_t consumed = 0;

    for (uint32_t row_index = 0; row_index < row_count; ++row_index) {
        const uint32_t length = ReadStream<uint32_t>(in);
        consumed += sizeof(uint32_t);

        std::string value(length, '\0');
        ReadBytes(in, value.data(), value.size());

        consumed += length;
        values_.push_back(std::move(value));
    }

    if (consumed != size) {
        throw Error::InconsistentData(ModuleName(), "column chunk size mismatch");
    }
}
