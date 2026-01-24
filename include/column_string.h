#pragma once

#include <vector>

#include "column.h"

class StringColumn final : public Column {
   public:
    StringColumn();

    size_t Size() const override;
    void Reserve(size_t n) override;
    void Clear() override;

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;

    void WriteTo(std::ostream& out) const override;
    void ReadFrom(std::istream& in, uint32_t row_count, uint64_t size) override;

   private:
    std::vector<std::string> values_;
};
