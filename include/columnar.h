#pragma once

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

void ConvertCsvToColumnar(const std::filesystem::path& schema_path, const std::filesystem::path& data_path,
                          const std::filesystem::path& output_path, size_t max_rows_per_group = 4);
void ConvertColumnarToCsv(const std::filesystem::path& columnar_path, const std::filesystem::path& schema_path,
                          const std::filesystem::path& data_path);
