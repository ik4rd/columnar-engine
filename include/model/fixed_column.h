#pragma once

#include <span>
#include <vector>

#include "common/error.h"
#include "io/stream.h"
#include "model/column.h"
#include "model/column_traits.h"

template <class ColumnImpl, std::integral T, ColumnType TypeValue>
class FixedColumn : public MutableColumn {
   public:
    FixedColumn() : MutableColumn(TypeValue) {}
    FixedColumn(const FixedColumn&) = default;
    FixedColumn(FixedColumn&&) noexcept = default;
    FixedColumn& operator=(const FixedColumn&) = default;
    FixedColumn& operator=(FixedColumn&&) noexcept = default;
    ~FixedColumn() override = default;

    size_t Size() const override { return values_.size(); }
    void Reserve(const size_t n) override { values_.reserve(n); }
    void Clear() override { values_.clear(); }

    void AppendFromString(const std::string_view value) override {
        AppendValue(ColumnValueTraits<TypeValue>::Parse(value));
    }

    void AppendFromColumn(const Column& source, const size_t row) override {
        if (source.Type() != TypeValue) {
            throw Error::InconsistentData(ColumnImpl::ModuleName(), "column type mismatch");
        }
        const auto& typed_source = static_cast<const FixedColumn&>(source);
        AppendValue(typed_source.ValueAt(row));
    }

    void AppendRangeFromColumn(const Column& source, const size_t begin, const size_t count) override {
        if (source.Type() != TypeValue) {
            throw Error::InconsistentData(ColumnImpl::ModuleName(), "column type mismatch");
        }
        const auto& typed_source = static_cast<const FixedColumn&>(source);
        if (begin > typed_source.values_.size() || count > typed_source.values_.size() - begin) {
            throw Error::OutOfRange(ColumnImpl::ModuleName(), "row range out of range");
        }
        values_.insert(values_.end(), typed_source.values_.begin() + static_cast<std::ptrdiff_t>(begin),
                       typed_source.values_.begin() + static_cast<std::ptrdiff_t>(begin + count));
    }

    void AppendSelectedFromColumn(const Column& source, const std::span<const size_t> rows) override {
        if (source.Type() != TypeValue) {
            throw Error::InconsistentData(ColumnImpl::ModuleName(), "column type mismatch");
        }
        const auto& typed_source = static_cast<const FixedColumn&>(source);
        values_.reserve(values_.size() + rows.size());
        for (const size_t row : rows) {
            values_.push_back(typed_source.ValueAt(row));
        }
    }

    std::string ValueAsString(const size_t row) const override {
        return ColumnValueTraits<TypeValue>::ToString(ValueAt(row));
    }

    Int128 ValueAsInt128(const size_t row) const override { return static_cast<Int128>(ValueAt(row)); }

    void AppendEncodedValue(const size_t row, std::string& out) const override {
        const T value = ValueAt(row);
        out.append(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    void SelectRowsByInt128Comparison(const Int128 rhs, const ValueComparison comparison,
                                      std::vector<size_t>& rows) const override {
        for (size_t row = 0; row < values_.size(); ++row) {
            if (MatchesValueComparison(static_cast<Int128>(values_[row]), rhs, comparison)) {
                rows.push_back(row);
            }
        }
    }

    std::unique_ptr<Column> Clone() const override {
        return std::make_unique<ColumnImpl>(static_cast<const ColumnImpl&>(*this));
    }

    std::unique_ptr<MutableColumn> CloneMutable() const override {
        return std::make_unique<ColumnImpl>(static_cast<const ColumnImpl&>(*this));
    }

    void WriteTo(std::ostream& out) const override {
        WriteBytes(out, {reinterpret_cast<const char*>(values_.data()), values_.size() * sizeof(T)});
    }

    void ReadFrom(std::istream& in, const uint32_t row_count, const uint64_t size) override {
        const uint64_t expected = static_cast<uint64_t>(row_count) * sizeof(T);
        if (size != expected) {
            throw Error::InconsistentData(ColumnImpl::ModuleName(), "column chunk size mismatch");
        }
        values_.resize(row_count);
        ReadBytes(in, reinterpret_cast<char*>(values_.data()), values_.size() * sizeof(T));
    }

   protected:
    void AppendValue(const T value) { values_.push_back(value); }

    static bool MatchesValueComparison(const Int128 lhs, const Int128 rhs, const ValueComparison comparison) {
        switch (comparison) {
            case ValueComparison::Equal:
                return lhs == rhs;
            case ValueComparison::NotEqual:
                return lhs != rhs;
            case ValueComparison::Less:
                return lhs < rhs;
            case ValueComparison::LessOrEqual:
                return lhs <= rhs;
            case ValueComparison::Greater:
                return lhs > rhs;
            case ValueComparison::GreaterOrEqual:
                return lhs >= rhs;
        }

        return false;
    }

    T ValueAt(const size_t row) const {
        CheckRowIndex(ColumnImpl::ModuleName(), row, values_.size());
        return values_[row];
    }

    std::vector<T> values_;
};
