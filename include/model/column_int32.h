#pragma once

#include "model/fixed_column.h"

class Int32Column final : public FixedColumn<Int32Column, int32_t, ColumnType::Int32> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_int32"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
