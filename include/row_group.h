#pragma once

#include <string>
#include <vector>

#include "columnar.h"
#include "fileio.h"
#include "metadata.h"

struct ColumnBuffer {
    ColumnType type = ColumnType::String;

    std::vector<int64_t> int_values;
    std::vector<std::string> string_values;

    void Clear() {
        int_values.clear();
        string_values.clear();
    }
};

void FlushRowGroup(FileWriter& writer, std::vector<ColumnBuffer>& buffers, uint32_t row_count,
                   RowGroupMetadata& metadata);
