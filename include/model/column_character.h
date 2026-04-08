/* (What is this? — Колонка символьного типа) */

#pragma once

#include "fixed_column.h"

class CharacterColumn final : public FixedColumn<CharacterColumn, char, ColumnType::Character> {
   public:
    using FixedColumn::FixedColumn;

    static const char* ModuleName() { return "column_character"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
