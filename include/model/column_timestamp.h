#pragma once

#include "model/fixed_column.h"

class TimestampColumn final : public FixedColumn<TimestampColumn, int64_t, ColumnType::Timestamp> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_timestamp"; }
};
