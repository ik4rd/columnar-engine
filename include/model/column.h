#pragma once

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

#include "common/int128.h"
#include "model/schema.h"

class Column {
   public:
    explicit Column(ColumnType type);
    Column(const Column&) = default;
    Column(Column&&) noexcept = default;
    Column& operator=(const Column&) = default;
    Column& operator=(Column&&) noexcept = default;
    virtual ~Column() = default;

    ColumnType Type() const { return type_; }

    virtual size_t Size() const = 0;
    virtual std::string ValueAsString(size_t row) const = 0;
    virtual Int128 ValueAsInt128(size_t row) const;
    virtual std::unique_ptr<Column> Clone() const = 0;

    virtual void WriteTo(std::ostream& out) const = 0;

   protected:
    static void CheckRowIndex(const char* module, size_t row, size_t size);

   private:
    ColumnType type_;
};

class MutableColumn : public Column {
   public:
    using Column::Column;
    ~MutableColumn() override = default;

    virtual void Reserve(size_t n) = 0;
    virtual void Clear() = 0;
    virtual void AppendFromString(std::string_view value) = 0;
    virtual void AppendFromColumn(const Column& source, size_t row) = 0;
    virtual void ReadFrom(std::istream& in, uint32_t row_count, uint64_t size) = 0;
    virtual std::unique_ptr<MutableColumn> CloneMutable() const = 0;
};

std::unique_ptr<MutableColumn> CreateColumn(ColumnType type);
