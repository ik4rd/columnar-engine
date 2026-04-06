#include "batch.h"

#include <utility>

#include "error.h"

static std::vector<std::unique_ptr<Column>> CloneColumns(const std::vector<std::unique_ptr<Column>>& columns) {
    std::vector<std::unique_ptr<Column>> clones;
    clones.reserve(columns.size());
    for (const auto& column : columns) {
        clones.push_back(column->Clone());
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

const Column& Batch::ColumnAt(const size_t i) const {
    if (i >= columns_.size()) {
        throw Error::OutOfRange("batch", "column index out of range");
    }
    return *columns_[i];
}

Column& Batch::ColumnAt(const size_t i) {
    if (i >= columns_.size()) {
        throw Error::OutOfRange("batch", "column index out of range");
    }
    return *columns_[i];
}

void Batch::Validate() const {
    if (schema_.columns.size() != columns_.size()) {
        throw Error::Mismatch("batch", "column count mismatch");
    }

    if (columns_.empty()) {
        return;
    }

    const size_t rows = columns_.front()->Size();
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i]->Type() != schema_.columns[i].type) {
            throw Error::Mismatch("batch", "column type mismatch");
        }
        if (columns_[i]->Size() != rows) {
            throw Error::Mismatch("batch", "column row count mismatch");
        }
    }
}
