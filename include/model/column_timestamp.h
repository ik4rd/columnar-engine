/* (What is this? — Колонка типа TIMESTAMP) */

#pragma once

#include "fixed_column.h"

class TimestampColumn final : public FixedColumn<TimestampColumn, int64_t, ColumnType::Timestamp> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_timestamp"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
