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

static bool LikeMatchesPercentOnly(const std::string_view value, const std::string_view pattern) {
    std::vector<std::string_view> segments;

    for (size_t pos = 0; pos < pattern.size();) {
        while (pos < pattern.size() && pattern[pos] == '%') {
            ++pos;
        }

        const size_t start = pos;
        while (pos < pattern.size() && pattern[pos] != '%') {
            ++pos;
        }

        if (start != pos) {
            segments.push_back(pattern.substr(start, pos - start));
        }
    }

    if (segments.empty()) {
        return true;
    }

    size_t value_pos = 0;
    size_t segment_index = 0;

    if (pattern.front() != '%') {
        if (!value.starts_with(segments.front())) {
            return false;
        }
        value_pos = segments.front().size();
        segment_index = 1;
    }

    for (; segment_index < segments.size(); ++segment_index) {
        const std::string_view segment = segments[segment_index];
        const size_t found = value.find(segment, value_pos);
        if (found == std::string_view::npos) {
            return false;
        }
        value_pos = found + segment.size();
    }

    return pattern.back() == '%' || value.ends_with(segments.back());
}

static bool LikeMatchesWithUnderscore(const std::string_view value, const std::string_view pattern) {
    size_t value_pos = 0;
    size_t pattern_pos = 0;
    size_t last_percent = std::string_view::npos;
    size_t retry_value_pos = 0;

    while (value_pos < value.size()) {
        if (pattern_pos < pattern.size() && (pattern[pattern_pos] == '_' || pattern[pattern_pos] == value[value_pos])) {
            ++value_pos;
            ++pattern_pos;
        } else if (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
            last_percent = pattern_pos++;
            retry_value_pos = value_pos;
        } else if (last_percent != std::string_view::npos) {
            pattern_pos = last_percent + 1;
            value_pos = ++retry_value_pos;
        } else {
            return false;
        }
    }

    while (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
        ++pattern_pos;
    }

    return pattern_pos == pattern.size();
}

static bool LikeMatches(const std::string_view value, const std::string_view pattern) {
    if (pattern.find('_') == std::string_view::npos) {
        return LikeMatchesPercentOnly(value, pattern);
    }

    return LikeMatchesWithUnderscore(value, pattern);
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
    out.push_back(':');
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
