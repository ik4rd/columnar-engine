#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

enum class ColumnType : uint8_t {
    Int64 = 1,
    String = 2,
};

struct ColumnSchema {
    std::string name;
    ColumnType type = ColumnType::String;
};

struct Schema {
    std::vector<ColumnSchema> columns;
};

Schema ReadSchemaCsv(const std::filesystem::path& path);
void WriteSchemaCsv(const std::filesystem::path& path, const Schema& schema);
