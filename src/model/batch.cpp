#include "model/batch.h"

#include <utility>

#include "common/error.h"

static std::vector<std::unique_ptr<MutableColumn>> CloneColumns(
    const std::vector<std::unique_ptr<MutableColumn>>& columns) {
    std::vector<std::unique_ptr<MutableColumn>> clones;
    clones.reserve(columns.size());
    for (const auto& column : columns) {
        clones.push_back(column->CloneMutable());
    }
    return clones;
}

Batch::Batch(Schema schema) : schema_(std::move(schema)) {
    columns_.reserve(schema_.columns.size());
    for (const auto& [name, type] : schema_.columns) {
        columns_.push_back(CreateColumn(type));
    }
}

Batch::Batch(Schema schema, const size_t reserve_rows) : Batch(std::move(schema)) { Reserve(reserve_rows); }

Batch::Batch(const Batch& other) : schema_(other.schema_), columns_(CloneColumns(other.columns_)) {}

Batch& Batch::operator=(const Batch& other) {
    if (this == &other) {
        return *this;
    }

    auto columns = CloneColumns(other.columns_);
    schema_ = other.schema_;
    columns_ = std::move(columns);

    return *this;
}

size_t Batch::ColumnsCount() const { return columns_.size(); }

size_t Batch::RowsCount() const {
    if (columns_.empty()) {
        return 0;
    }
    return columns_.front()->Size();
}

void Batch::Reserve(const size_t n) const {
    for (auto& column : columns_) {
        column->Reserve(n);
    }
}

void Batch::AppendValueFromString(const size_t column_index, const std::string& value) const {
    if (column_index >= columns_.size()) {
        throw Error::OutOfRange("batch", "column index out of range");
    }
    columns_[column_index]->AppendFromString(value);
}

void Batch::ReadColumnFrom(const size_t column_index, std::istream& in, const uint32_t row_count,
                           const uint64_t size) const {
    if (column_index >= columns_.size()) {
        throw Error::OutOfRange("batch", "column index out of range");
    }
    columns_[column_index]->ReadFrom(in, row_count, size);
}

const Column& Batch::ColumnAt(const size_t i) const {
    if (i >= columns_.size()) {
        throw Error::OutOfRange("batch", "column index out of range");
    }
    return *columns_[i];
}

void Batch::Validate() const {
    if (schema_.columns.size() != columns_.size()) {
        throw Error::InconsistentData("batch", "column count mismatch");
    }

    if (columns_.empty()) {
        return;
    }

    const size_t rows = columns_.front()->Size();
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i]->Type() != schema_.columns[i].type) {
            throw Error::InconsistentData("batch", "column type mismatch");
        }
        if (columns_[i]->Size() != rows) {
            throw Error::InconsistentData("batch", "column row count mismatch");
        }
    }
}
