#include "batch.h"

#include <utility>

#include "error.h"

Batch::Batch(Schema schema) : schema_(std::move(schema)) {
    columns_.reserve(schema_.columns.size());
    for (const auto& [name, type] : schema_.columns) {
        columns_.push_back(CreateColumn(type));
    }
}

Batch::Batch(Schema schema, const size_t reserve_rows) : Batch(std::move(schema)) { Reserve(reserve_rows); }

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
        throw error::MakeError("batch", "column index out of range");
    }
    return *columns_[i];
}

Column& Batch::ColumnAt(const size_t i) {
    if (i >= columns_.size()) {
        throw error::MakeError("batch", "column index out of range");
    }
    return *columns_[i];
}

void Batch::Validate() const {
    if (schema_.columns.size() != columns_.size()) {
        throw error::MakeError("batch", "column count mismatch");
    }

    if (columns_.empty()) {
        return;
    }

    const size_t rows = columns_.front()->Size();
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i]->Type() != schema_.columns[i].type) {
            throw error::MakeError("batch", "column type mismatch");
        }
        if (columns_[i]->Size() != rows) {
            throw error::MakeError("batch", "column row count mismatch");
        }
    }
}
