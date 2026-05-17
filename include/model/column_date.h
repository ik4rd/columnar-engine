#pragma once

#include "model/fixed_column.h"

class DateColumn final : public FixedColumn<DateColumn, int32_t, ColumnType::Date> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_date"; }
};
