#pragma once

#include "model/fixed_column.h"

class CharacterColumn final : public FixedColumn<CharacterColumn, char, ColumnType::Character> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_character"; }
};
