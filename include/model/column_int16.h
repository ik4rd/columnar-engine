/* (What is this? — Колонка типа INT16) */

#pragma once

#include "fixed_column.h"

class Int16Column final : public FixedColumn<Int16Column, int16_t, ColumnType::Int16> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_int16"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
