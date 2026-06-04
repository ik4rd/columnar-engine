#include "model/column_string.h"

#include <cstring>
#include <limits>
#include <memory>
#include <unordered_set>
#include <utility>

#include "common/error.h"
#include "common/string_pattern_utils.h"
#include "io/stream.h"

constexpr char EncodedValueSeparator = ':';

StringColumn::StringColumn() : MutableColumn(ColumnType::String), offsets_{0} {}

size_t StringColumn::Size() const { return offsets_.empty() ? 0 : offsets_.size() - 1; }

void StringColumn::Reserve(const size_t n) { offsets_.reserve(n + 1); }

void StringColumn::Clear() {
    blob_.clear();
    offsets_.clear();
    offsets_.push_back(0);
}

void StringColumn::AppendFromString(const std::string_view value) { AppendValue(value); }

void StringColumn::AppendFromColumn(const Column& source, const size_t row) {
    if (source.Type() != ColumnType::String) {
        throw Error::InconsistentData(ModuleName(), "column type mismatch");
    }

    if (&source == this) {
        const std::string copy{StringAt(row)};
        AppendValue(copy);

        return;
    }

    std::string scratch;
    AppendValue(source.ValueAsStringView(row, scratch));
}

void StringColumn::AppendRangeFromColumn(const Column& source, const size_t begin, const size_t count) {
    if (source.Type() != ColumnType::String) {
        throw Error::InconsistentData(ModuleName(), "column type mismatch");
    }

    const auto& typed_source = static_cast<const StringColumn&>(source);

    if (begin > typed_source.Size() || count > typed_source.Size() - begin) {
        throw Error::OutOfRange(ModuleName(), "row range out of range");
    }
    if (count == 0) {
        return;
    }

    const uint32_t source_begin = typed_source.offsets_[begin];
    const uint32_t source_end = typed_source.offsets_[begin + count];
    const size_t bytes_to_copy = source_end - source_begin;
    CheckAppendSize(bytes_to_copy);

    if (&typed_source == this) {
        const std::vector<char> blob_slice(blob_.begin() + source_begin, blob_.begin() + source_end);
        const std::vector<uint32_t> offset_slice(offsets_.begin() + static_cast<std::ptrdiff_t>(begin),
                                                 offsets_.begin() + static_cast<std::ptrdiff_t>(begin + count + 1));

        const uint32_t base = static_cast<uint32_t>(blob_.size());
        blob_.insert(blob_.end(), blob_slice.begin(), blob_slice.end());
        offsets_.reserve(offsets_.size() + count);

        for (size_t i = 1; i <= count; ++i) {
            offsets_.push_back(base + offset_slice[i] - source_begin);
        }

        return;
    }

    const uint32_t base = static_cast<uint32_t>(blob_.size());
    blob_.insert(blob_.end(), typed_source.blob_.begin() + source_begin, typed_source.blob_.begin() + source_end);
    offsets_.reserve(offsets_.size() + count);

    for (size_t i = 1; i <= count; ++i) {
        offsets_.push_back(base + typed_source.offsets_[begin + i] - source_begin);
    }
}

void StringColumn::AppendSelectedFromColumn(const Column& source, const std::span<const size_t> rows) {
    if (source.Type() != ColumnType::String) {
        throw Error::InconsistentData(ModuleName(), "column type mismatch");
    }

    const auto& typed_source = static_cast<const StringColumn&>(source);
    offsets_.reserve(offsets_.size() + rows.size());

    if (&typed_source == this) {
        std::vector<std::string> snapshot;
        snapshot.reserve(rows.size());

        for (const size_t row : rows) {
            snapshot.emplace_back(StringAt(row));
        }

        for (const std::string& value : snapshot) {
            AppendValue(value);
        }

        return;
    }

    for (const size_t row : rows) {
        AppendValue(typed_source.StringAt(row));
    }
}

std::string StringColumn::ValueAsString(const size_t row) const {
    const std::string_view value = StringAt(row);

    return {value.data(), value.size()};
}

std::string_view StringColumn::ValueAsStringView(const size_t row, std::string& /*scratch*/) const {
    return StringAt(row);
}

void StringColumn::SelectRowsByStringSet(const std::unordered_set<std::string>& values,
                                         std::vector<size_t>& rows) const {
    std::unordered_set<std::string_view> value_views;
    value_views.reserve(values.size());

    for (const std::string& value : values) {
        value_views.insert(value);
    }

    for (size_t row = 0; row < Size(); ++row) {
        if (value_views.contains(StringAt(row))) {
            rows.push_back(row);
        }
    }
}

void StringColumn::SelectRowsByLikePattern(const std::string_view pattern, const bool negated,
                                           std::vector<size_t>& rows) const {
    for (size_t row = 0; row < Size(); ++row) {
        const bool matched = LikeMatches(StringAt(row), pattern);
        if (negated ? !matched : matched) {
            rows.push_back(row);
        }
    }
}

void StringColumn::AppendEncodedValue(const size_t row, std::string& out) const {
    const std::string_view value = StringAt(row);
    out += std::to_string(value.size());
    out.push_back(EncodedValueSeparator);
    out.append(value.data(), value.size());
}

std::unique_ptr<Column> StringColumn::Clone() const { return std::make_unique<StringColumn>(*this); }

std::unique_ptr<MutableColumn> StringColumn::CloneMutable() const { return std::make_unique<StringColumn>(*this); }

void StringColumn::WriteTo(std::ostream& out) const {
    for (size_t row = 0; row < Size(); ++row) {
        const std::string_view value = StringAt(row);

        if (value.size() > std::numeric_limits<uint32_t>::max()) {
            throw Error::Overflow(ModuleName(), "value exceeds supported size");
        }

        WriteStream<uint32_t>(out, static_cast<uint32_t>(value.size()));
        WriteBytes(out, value);
    }
}

void StringColumn::ReadFrom(const std::span<const char> data, const uint32_t row_count, const uint64_t size) {
    if (static_cast<uint64_t>(data.size()) != size) {
        throw Error::InconsistentData(ModuleName(), "column chunk size mismatch");
    }

    blob_.clear();
    blob_.reserve(data.size());

    offsets_.clear();
    offsets_.reserve(static_cast<size_t>(row_count) + 1);
    offsets_.push_back(0);

    const char* ptr = data.empty() ? "" : data.data();
    const char* const end = ptr + data.size();

    for (uint32_t row_index = 0; row_index < row_count; ++row_index) {
        if (static_cast<size_t>(end - ptr) < sizeof(uint32_t)) {
            throw Error::InconsistentData(ModuleName(), "column chunk size mismatch");
        }

        uint32_t length = 0;
        std::memcpy(&length, ptr, sizeof(length));
        ptr += sizeof(length);

        if (static_cast<size_t>(end - ptr) < length) {
            throw Error::InconsistentData(ModuleName(), "column chunk size mismatch");
        }

        AppendValue({ptr, length});
        ptr += length;
    }

    if (ptr != end) {
        throw Error::InconsistentData(ModuleName(), "column chunk size mismatch");
    }
}

std::string_view StringColumn::StringAt(const size_t row) const {
    CheckRowIndex(ModuleName(), row, Size());
    const uint32_t begin = offsets_[row];
    const uint32_t end = offsets_[row + 1];

    return {begin == end ? "" : blob_.data() + begin, end - begin};
}

void StringColumn::AppendValue(const std::string_view value) {
    CheckAppendSize(value.size());
    blob_.insert(blob_.end(), value.begin(), value.end());
    offsets_.push_back(static_cast<uint32_t>(blob_.size()));
}

void StringColumn::CheckAppendSize(const size_t value_size) const {
    constexpr size_t MaxBlobSize = std::numeric_limits<uint32_t>::max();

    if (value_size > MaxBlobSize || blob_.size() > MaxBlobSize - value_size) {
        throw Error::Overflow(ModuleName(), "string column blob exceeds supported size");
    }
}
