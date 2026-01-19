#include "row_group.h"

#include <limits>

#include "error.h"
#include "stream.h"

void FlushRowGroup(FileWriter& writer, std::vector<ColumnBuffer>& buffers, const uint32_t row_count,
                   RowGroupMetadata& metadata) {
    auto& out = writer.Stream();
    metadata.row_count = row_count;
    metadata.columns.clear();
    metadata.columns.reserve(buffers.size());

    for (ColumnBuffer& buffer : buffers) {
        const uint64_t offset = writer.Tell();

        if (buffer.type == ColumnType::Int64) {
            if (buffer.int_values.size() != row_count) {
                throw error::MakeError("columnar", "row group has mismatched int64 column size");
            }
            for (const int64_t value : buffer.int_values) {
                WriteLittleEndian<int64_t>(out, value);
            }
        } else if (buffer.type == ColumnType::String) {
            if (buffer.string_values.size() != row_count) {
                throw error::MakeError("columnar", "row group has mismatched string column size");
            }
            for (const std::string& value : buffer.string_values) {
                if (value.size() > std::numeric_limits<uint32_t>::max()) {
                    throw error::MakeError("columnar", "string value exceeds supported size");
                }
                WriteLittleEndian<uint32_t>(out, static_cast<uint32_t>(value.size()));
                WriteBytes(out, value);
            }
        } else {
            throw error::MakeError("columnar", "unsupported column type in row group");
        }

        const uint64_t end = writer.Tell();
        metadata.columns.push_back(ColumnChunkMetadata{offset, end - offset});

        buffer.Clear();
    }
}
