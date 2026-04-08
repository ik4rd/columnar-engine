/* (What is this? — Колонка типа INT128) */

#pragma once

#if !defined(__SIZEOF_INT128__)
#error "This project requires compiler support for 128-bit integers"
#endif

#include "fixed_column.h"

class Int128Column final : public FixedColumn<Int128Column, __int128_t, ColumnType::Int128> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_int128"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
