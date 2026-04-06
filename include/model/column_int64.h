/* (What is this? — Колонка типа INT64) */

#pragma once

#include <vector>

#include "column.h"

class Int64Column final : public Column {
   public:
    Int64Column();
    Int64Column(const Int64Column&) = default;
    Int64Column(Int64Column&&) noexcept = default;
    Int64Column& operator=(const Int64Column&) = default;
    Int64Column& operator=(Int64Column&&) noexcept = default;
    ~Int64Column() override = default;

    size_t Size() const override;
    void Reserve(size_t n) override;
    void Clear() override;

    void AppendFromString(const std::string& value) override;
    uint64_t EstimateSizeFromString(std::string_view value) const override;
    std::string ValueAsString(size_t row) const override;
    std::unique_ptr<Column> Clone() const override;

    void WriteTo(std::ostream& out) const override;
    void ReadFrom(std::istream& in, uint32_t row_count, uint64_t size) override;

   private:
    std::vector<int64_t> values_;
};
