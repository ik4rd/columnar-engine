/* (What is this? — Колонка логического типа) */

#pragma once

#include "fixed_column.h"

class BooleanColumn final : public FixedColumn<BooleanColumn, uint8_t, ColumnType::Boolean> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_boolean"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
