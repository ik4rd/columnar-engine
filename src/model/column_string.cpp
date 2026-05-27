#include "model/column_string.h"

#include <limits>
#include <memory>
#include <utility>

#include "common/error.h"
#include "common/operator_utils.h"
#include "io/stream.h"

constexpr char EncodedValueSeparator = ':';

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

void StringColumn::AppendRangeFromColumn(const Column& source, const size_t begin, const size_t count) {
    if (source.Type() != ColumnType::String) {
        throw Error::InconsistentData(ModuleName(), "column type mismatch");
    }
    const auto& typed_source = static_cast<const StringColumn&>(source);
    if (begin > typed_source.values_.size() || count > typed_source.values_.size() - begin) {
        throw Error::OutOfRange(ModuleName(), "row range out of range");
    }
    values_.insert(values_.end(), typed_source.values_.begin() + static_cast<std::ptrdiff_t>(begin),
                   typed_source.values_.begin() + static_cast<std::ptrdiff_t>(begin + count));
}

void StringColumn::AppendSelectedFromColumn(const Column& source, const std::span<const size_t> rows) {
    if (source.Type() != ColumnType::String) {
        throw Error::InconsistentData(ModuleName(), "column type mismatch");
    }
    const auto& typed_source = static_cast<const StringColumn&>(source);
    values_.reserve(values_.size() + rows.size());
    for (const size_t row : rows) {
        CheckRowIndex(ModuleName(), row, typed_source.values_.size());
        values_.push_back(typed_source.values_[row]);
    }
}

std::string StringColumn::ValueAsString(const size_t row) const {
    CheckRowIndex(ModuleName(), row, values_.size());
    return values_[row];
}

void StringColumn::SelectRowsByStringSet(const std::unordered_set<std::string>& values,
                                         std::vector<size_t>& rows) const {
    for (size_t row = 0; row < values_.size(); ++row) {
        if (values.contains(values_[row])) {
            rows.push_back(row);
        }
    }
}

void StringColumn::SelectRowsByLikePattern(const std::string_view pattern, const bool negated,
                                           std::vector<size_t>& rows) const {
    for (size_t row = 0; row < values_.size(); ++row) {
        const bool matched = LikeMatches(values_[row], pattern);
        if (negated ? !matched : matched) {
            rows.push_back(row);
        }
    }
}

void StringColumn::AppendEncodedValue(const size_t row, std::string& out) const {
    CheckRowIndex(ModuleName(), row, values_.size());
    const std::string& value = values_[row];
    out += std::to_string(value.size());
    out.push_back(EncodedValueSeparator);
    out += value;
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
        consumed += sizeof(length);

        std::string value(length, '\0');
        ReadBytes(in, value.data(), value.size());

        consumed += length;
        values_.push_back(std::move(value));
    }

    if (consumed != size) {
        throw Error::InconsistentData(ModuleName(), "column chunk size mismatch");
    }
}
