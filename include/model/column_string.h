#pragma once

#include "model/variant_column.h"

class StringColumn final : public VariantColumn<StringColumn, ColumnType::String> {
   public:
    using VariantColumn::VariantColumn;

    static const char* ModuleName() { return "column_string"; }

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
};
