/* (What is this? — Колонка типа DATE) */

#pragma once

#include "fixed_column.h"

class DateColumn final : public FixedColumn<DateColumn, int32_t, ColumnType::Date> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_date"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
