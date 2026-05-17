#pragma once

#include "model/fixed_column.h"

class Int64Column final : public FixedColumn<Int64Column, int64_t, ColumnType::Int64> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_int64"; }
};
