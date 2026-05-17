#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

enum class ColumnType : uint8_t {
    Int64 = 1,
    String = 2,
    Boolean = 3,
    Int16 = 4,
    Int32 = 5,
    Int128 = 6,
    Date = 7,
    Timestamp = 8,
    Character = 9,
};

struct ColumnSchema {
    std::string name;
    ColumnType type;

    ColumnSchema(std::string column_name, const ColumnType column_type)
        : name(std::move(column_name)), type(column_type) {}

    bool operator==(const ColumnSchema& other) const = default;
};

struct Schema {
    std::vector<ColumnSchema> columns;

    bool operator==(const Schema& other) const = default;
};
