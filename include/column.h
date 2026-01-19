#pragma once

#include <iosfwd>
#include <memory>
#include <string>

#include "columnar.h"

class Column {
   public:
    explicit Column(ColumnType type);
    virtual ~Column() = default;

    ColumnType Type() const { return type_; }

    virtual size_t Size() const = 0;
    virtual void Reserve(size_t n) = 0;
    virtual void Clear() = 0;

    virtual void AppendFromString(const std::string& value) = 0;
    virtual std::string ValueAsString(size_t row) const = 0;

    virtual void WriteTo(std::ostream& out) const = 0;
    virtual void ReadFrom(std::istream& in, uint32_t row_count, uint64_t size) = 0;

   private:
    ColumnType type_;
};

std::unique_ptr<Column> CreateColumn(ColumnType type);
