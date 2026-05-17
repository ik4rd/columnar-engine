#pragma once

#include "model/fixed_column.h"

class BooleanColumn final : public FixedColumn<BooleanColumn, uint8_t, ColumnType::Boolean> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_boolean"; }
};
