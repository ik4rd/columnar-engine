#pragma once

#include <cstdint>
#include <iosfwd>
#include <vector>

#include "common/int128.h"
#include "io/compression.h"
#include "model/schema.h"

struct ColumnChunkMetadata {
    uint64_t offset = 0;

    uint64_t compressed_size = 0;
    uint64_t uncompressed_size = 0;
    Compression compression = Compression::None;

    bool has_min_max = false;

    Int128 min_value = 0;
    Int128 max_value = 0;
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
