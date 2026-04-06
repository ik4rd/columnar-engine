#include "metadata.h"

#include <limits>
#include <string>

#include "error.h"
#include "stream.h"

ColumnarMetadata ReadMetadata(std::istream& in) {
    ColumnarMetadata metadata;

    const uint32_t column_count = ReadStream<uint32_t>(in);
    metadata.schema.columns.reserve(column_count);

    for (uint32_t i = 0; i < column_count; ++i) {
        const uint32_t name_size = ReadStream<uint32_t>(in);
        std::string name(name_size, '\0');

        ReadBytes(in, name.data(), name.size());
        const uint8_t type_byte = ReadStream<uint8_t>(in);

        auto type = ColumnType::String;
        if (type_byte == static_cast<uint8_t>(ColumnType::Int64)) {
            type = ColumnType::Int64;
        } else if (type_byte == static_cast<uint8_t>(ColumnType::String)) {
            type = ColumnType::String;
        } else {
            throw error::MakeError("columnar", "unknown column type in metadata");
        }

        metadata.schema.columns.push_back(ColumnSchema{std::move(name), type});
    }

    const uint32_t row_group_count = ReadStream<uint32_t>(in);
    metadata.row_groups.reserve(row_group_count);

    for (uint32_t i = 0; i < row_group_count; ++i) {
        RowGroupMetadata group;
        group.row_count = ReadStream<uint32_t>(in);

        const uint32_t columns_in_group = ReadStream<uint32_t>(in);
        if (columns_in_group != column_count) {
            throw error::MakeError("columnar", "row group column count mismatch");
        }

        group.columns.reserve(columns_in_group);

        for (uint32_t col = 0; col < columns_in_group; ++col) {
            ColumnChunkMetadata chunk;
            chunk.offset = ReadStream<uint64_t>(in);
            chunk.size = ReadStream<uint64_t>(in);
            group.columns.push_back(chunk);
        }

        metadata.row_groups.push_back(std::move(group));
    }

    return metadata;
}

void WriteMetadata(std::ostream& out, const ColumnarMetadata& metadata) {
    if (metadata.schema.columns.size() > std::numeric_limits<uint32_t>::max()) {
        throw error::MakeError("columnar", "too many columns for metadata");
    }

    WriteStream<uint32_t>(out, metadata.schema.columns.size());
    for (const auto& [name, type] : metadata.schema.columns) {
        if (name.size() > std::numeric_limits<uint32_t>::max()) {
            throw error::MakeError("columnar", "column name too long");
        }
        WriteStream<uint32_t>(out, name.size());
        WriteBytes(out, name);
        WriteStream<uint8_t>(out, static_cast<uint8_t>(type));
    }

    if (metadata.row_groups.size() > std::numeric_limits<uint32_t>::max()) {
        throw error::MakeError("columnar", "too many row groups for metadata");
    }

    WriteStream<uint32_t>(out, metadata.row_groups.size());
    for (const auto& [row_count, columns] : metadata.row_groups) {
        WriteStream<uint32_t>(out, row_count);
        WriteStream<uint32_t>(out, columns.size());
        for (const auto& [offset, size] : columns) {
            WriteStream<uint64_t>(out, offset);
            WriteStream<uint64_t>(out, size);
        }
    }
}
