#pragma once

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "model/column.h"
#include "model/schema.h"

class Batch {
   public:
    Batch() = default;
    explicit Batch(Schema schema);
    Batch(Schema schema, size_t reserve_rows);
    Batch(const Batch& other);
    Batch(Batch&& other) noexcept = default;
    Batch& operator=(const Batch& other);
    Batch& operator=(Batch&& other) noexcept = default;
    ~Batch() = default;

   public:
    const Schema& GetSchema() const { return schema_; }

    size_t ColumnsCount() const;
    size_t RowsCount() const;

    void Reserve(size_t n) const;
    void AppendValueFromString(size_t column_index, const std::string& value) const;
    void ReadColumnFrom(size_t column_index, std::istream& in, uint32_t row_count, uint64_t size) const;

    const Column& ColumnAt(size_t i) const;

    void Validate() const;

   private:
    Schema schema_;
    std::vector<std::unique_ptr<MutableColumn>> columns_;
};
