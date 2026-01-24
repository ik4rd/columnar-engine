#pragma once

#include <memory>
#include <vector>

#include "column.h"
#include "schema.h"

class Batch {
   public:
    Batch() = default;
    explicit Batch(Schema schema);
    Batch(Schema schema, size_t reserve_rows);

    const Schema& GetSchema() const { return schema_; }

    size_t ColumnsCount() const;
    size_t RowsCount() const;

    void Reserve(size_t n) const;

    const Column& ColumnAt(size_t i) const;
    Column& ColumnAt(size_t i);

    const std::vector<std::unique_ptr<Column>>& GetColumns() const { return columns_; }
    std::vector<std::unique_ptr<Column>>& GetColumns() { return columns_; }

    void Validate() const;

   private:
    Schema schema_;
    std::vector<std::unique_ptr<Column>> columns_;
};
