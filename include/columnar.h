#pragma once

#include <cstddef>
#include <filesystem>

void ConvertCsvToColumnar(const std::filesystem::path& schema_path, const std::filesystem::path& data_path,
                          const std::filesystem::path& output_path, size_t max_rows_per_group = 4);
void ConvertColumnarToCsv(const std::filesystem::path& columnar_path, const std::filesystem::path& schema_path,
                          const std::filesystem::path& data_path);
