#include "columnar.h"

#include "fileio.h"

Schema ReadSchemaCsv(const std::filesystem::path& path) {
    (void)path;
    return Schema{};
}

void WriteSchemaCsv(const std::filesystem::path& path, const Schema& schema) {
    (void)path;
    (void)schema;
}

void ConvertCsvToColumnar(const std::filesystem::path& schema_path, const std::filesystem::path& data_path,
                          const std::filesystem::path& output_path, size_t max_rows_per_group) {
    (void)schema_path;
    (void)data_path;
    (void)output_path;
    (void)max_rows_per_group;
}

void ConvertColumnarToCsv(const std::filesystem::path& columnar_path, const std::filesystem::path& schema_path,
                          const std::filesystem::path& data_path) {
    (void)columnar_path;
    (void)schema_path;
    (void)data_path;
}

ColumnarMetadata ReadColumnarMetadata(const std::filesystem::path& path) {
    (void)path;
    return ColumnarMetadata{};
}
