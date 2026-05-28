#pragma once

#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
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

    void AppendFromString(std::string_view value) override;
    void AppendFromColumn(const Column& source, size_t row) override;
    void AppendRangeFromColumn(const Column& source, size_t begin, size_t count) override;
    void AppendSelectedFromColumn(const Column& source, std::span<const size_t> rows) override;
    std::string ValueAsString(size_t row) const override;
    void SelectRowsByStringSet(const std::unordered_set<std::string>& values, std::vector<size_t>& rows) const override;
    void SelectRowsByLikePattern(std::string_view pattern, bool negated, std::vector<size_t>& rows) const override;
    void AppendEncodedValue(size_t row, std::string& out) const override;

    std::unique_ptr<Column> Clone() const override;
    std::unique_ptr<MutableColumn> CloneMutable() const override;

    void WriteTo(std::ostream& out) const override;
    void ReadFrom(std::istream& in, uint32_t row_count, uint64_t size) override;

   private:
    std::vector<std::string> values_;
};
