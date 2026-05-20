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

void StringColumn::AppendFromString(const std::string_view value) { values_.emplace_back(value); }

void StringColumn::AppendFromColumn(const Column& source, const size_t row) {
    if (source.Type() != ColumnType::String) {
        throw Error::InconsistentData(ModuleName(), "column type mismatch");
    }
    const auto& typed_source = static_cast<const StringColumn&>(source);
    CheckRowIndex(ModuleName(), row, typed_source.values_.size());
    values_.push_back(typed_source.values_[row]);
}

std::string StringColumn::ValueAsString(const size_t row) const {
    CheckRowIndex(ModuleName(), row, values_.size());
    return values_[row];
}

std::unique_ptr<Column> StringColumn::Clone() const { return std::make_unique<StringColumn>(*this); }

std::unique_ptr<MutableColumn> StringColumn::CloneMutable() const { return std::make_unique<StringColumn>(*this); }

void StringColumn::WriteTo(std::ostream& out) const {
    for (const auto& value : values_) {
        if (value.size() > std::numeric_limits<uint32_t>::max()) {
            throw Error::Overflow(ModuleName(), "value exceeds supported size");
        }
        WriteStream<uint32_t>(out, static_cast<uint32_t>(value.size()));
        WriteBytes(out, value);
    }
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
