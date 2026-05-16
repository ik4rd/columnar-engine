#pragma once

#include "model/fixed_column.h"
#include "common/int128.h"

class Int128Column final : public FixedColumn<Int128Column, Int128, ColumnType::Int128> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_int128"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
