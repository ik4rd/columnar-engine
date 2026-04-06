/* (What is this? — Сериализация метаданных) */

#pragma once

#include <cstdint>
#include <iosfwd>
#include <vector>

#include "schema.h"

struct ColumnChunkMetadata {
    uint64_t offset = 0;
    uint64_t size = 0;
};

struct RowGroupMetadata {
    uint32_t row_count = 0;
    std::vector<ColumnChunkMetadata> columns;
};

struct ColumnarMetadata {
    Schema schema;
    std::vector<RowGroupMetadata> row_groups;
};

ColumnarMetadata ReadMetadata(std::istream& in);
void WriteMetadata(std::ostream& out, const ColumnarMetadata& metadata);
