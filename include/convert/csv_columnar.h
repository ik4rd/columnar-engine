#pragma once

#include <cstddef>
#include <filesystem>

#include "io/compression.h"

inline constexpr size_t DefaultMaxRowsPerGroup = 1 << 14;

void ConvertCsvToColumnar(const std::filesystem::path& schema_path, const std::filesystem::path& data_path,
                          const std::filesystem::path& output_path, size_t max_rows_per_group = DefaultMaxRowsPerGroup,
                          Compression compression = Compression::None);
void ConvertColumnarToCsv(const std::filesystem::path& columnar_path, const std::filesystem::path& schema_path,
                          const std::filesystem::path& data_path);
