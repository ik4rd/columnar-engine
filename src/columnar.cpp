#include "columnar.h"

#include "error.h"
#include "fileio.h"

namespace {
std::string ToLower(const std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (const unsigned char ch : input) {
        output.push_back(static_cast<char>(std::tolower(ch)));
    }
    return output;
}
[[maybe_unused]] ColumnType ParseColumnType(const std::string_view input) {
    const std::string lowered = ToLower(input);
    if (lowered == "int64") {
        return ColumnType::Int64;
    }
    if (lowered == "string") {
        return ColumnType::String;
    }
    throw error::MakeError("columnar", "unknown column type");
}
[[maybe_unused]] std::string ColumnTypeToString(const ColumnType type) {
    switch (type) {
        case ColumnType::Int64:
            return "int64";
        case ColumnType::String:
            return "string";
    }
    return "string";
}
}  // namespace

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
