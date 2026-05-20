#pragma once

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
    std::string ValueAsString(const size_t row) const override {
        return ColumnValueTraits<TypeValue>::ToString(ValueAt(row));
    }
    Int128 ValueAsInt128(const size_t row) const override { return static_cast<Int128>(ValueAt(row)); }

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

    T ValueAt(const size_t row) const {
        CheckRowIndex(ColumnImpl::ModuleName(), row, values_.size());
        return values_[row];
    }

    std::vector<T> values_;
};
