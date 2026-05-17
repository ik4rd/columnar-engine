#pragma once

#include <string>
#include <vector>

#include "model/column.h"

class StringColumn final : public MutableColumn {
   public:
    StringColumn();
    StringColumn(const StringColumn&) = default;
    StringColumn(StringColumn&&) noexcept = default;
    StringColumn& operator=(const StringColumn&) = default;
    StringColumn& operator=(StringColumn&&) noexcept = default;
    ~StringColumn() override = default;

    static const char* ModuleName() { return "column_string"; }

    size_t Size() const override;
    void Reserve(size_t n) override;
    void Clear() override;

    void AppendFromString(const std::string& value) override;
    std::string ValueAsString(size_t row) const override;
    std::unique_ptr<Column> Clone() const override;
    std::unique_ptr<MutableColumn> CloneMutable() const override;

    void WriteTo(std::ostream& out) const override;
    void ReadFrom(std::istream& in, uint32_t row_count, uint64_t size) override;

   private:
    std::vector<std::string> values_;
};
