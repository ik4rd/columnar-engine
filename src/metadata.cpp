#include "metadata.h"

#include <limits>
#include <string>

#include "error.h"
#include "fileio.h"
#include "magic.h"
#include "stream.h"

ColumnarMetadata ReadMetadata(std::istream& in) {
    ColumnarMetadata metadata;

    const uint32_t column_count = ReadLittleEndian<uint32_t>(in);
    metadata.schema.columns.reserve(column_count);

    for (uint32_t i = 0; i < column_count; ++i) {
        const uint32_t name_size = ReadLittleEndian<uint32_t>(in);
        std::string name(name_size, '\0');

        ReadBytes(in, name.data(), name.size());
        const uint8_t type_byte = ReadLittleEndian<uint8_t>(in);

        ColumnType type = ColumnType::String;
        if (type_byte == static_cast<uint8_t>(ColumnType::Int64)) {
            type = ColumnType::Int64;
        } else if (type_byte == static_cast<uint8_t>(ColumnType::String)) {
            type = ColumnType::String;
        } else {
            throw error::MakeError("columnar", "unknown column type in metadata");
        }

        metadata.schema.columns.push_back(ColumnSchema{std::move(name), type});
    }

    const uint32_t row_group_count = ReadLittleEndian<uint32_t>(in);
    metadata.row_groups.reserve(row_group_count);

    for (uint32_t i = 0; i < row_group_count; ++i) {
        RowGroupMetadata group;
        group.row_count = ReadLittleEndian<uint32_t>(in);

        const uint32_t columns_in_group = ReadLittleEndian<uint32_t>(in);
        if (columns_in_group != column_count) {
            throw error::MakeError("columnar", "row group column count mismatch");
        }

        group.columns.reserve(columns_in_group);

        for (uint32_t col = 0; col < columns_in_group; ++col) {
            ColumnChunkMetadata chunk;
            chunk.offset = ReadLittleEndian<uint64_t>(in);
            chunk.size = ReadLittleEndian<uint64_t>(in);
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

    WriteLittleEndian<uint32_t>(out, static_cast<uint32_t>(metadata.schema.columns.size()));
    for (const auto& [name, type] : metadata.schema.columns) {
        if (name.size() > std::numeric_limits<uint32_t>::max()) {
            throw error::MakeError("columnar", "column name too long");
        }
        WriteLittleEndian<uint32_t>(out, static_cast<uint32_t>(name.size()));
        WriteBytes(out, name);
        WriteLittleEndian<uint8_t>(out, static_cast<uint8_t>(type));
    }

    if (metadata.row_groups.size() > std::numeric_limits<uint32_t>::max()) {
        throw error::MakeError("columnar", "too many row groups for metadata");
    }

    WriteLittleEndian<uint32_t>(out, static_cast<uint32_t>(metadata.row_groups.size()));
    for (const auto& [row_count, columns] : metadata.row_groups) {
        WriteLittleEndian<uint32_t>(out, row_count);
        WriteLittleEndian<uint32_t>(out, static_cast<uint32_t>(columns.size()));
        for (const auto& [offset, size] : columns) {
            WriteLittleEndian<uint64_t>(out, offset);
            WriteLittleEndian<uint64_t>(out, size);
        }
    }
}

ColumnarMetadata ReadColumnarMetadata(const std::filesystem::path& path) {
    const auto metadata = GetFileMetadata(path);
    if (!metadata || !metadata->is_regular) {
        throw error::MakeError("columnar", "columnar file not found");
    }

    const uint64_t file_size = metadata->size;
    const std::string_view magic = ColumnarMagic();
    const uint64_t footer_size = FooterSize();
    if (file_size < footer_size) {
        throw error::MakeError("columnar", "columnar file is too small");
    }

    FileReader reader(path);
    auto& in = reader.Stream();
    in.seekg(static_cast<std::streamoff>(file_size - footer_size));
    if (!in) {
        throw error::MakeError("columnar", "failed to seek to footer");
    }

    const uint64_t metadata_size = ReadLittleEndian<uint64_t>(in);
    std::string magic_read(magic.size(), '\0');
    ReadBytes(in, magic_read.data(), magic_read.size());
    if (magic_read != magic) {
        throw error::MakeError("columnar", "invalid columnar magic");
    }

    if (metadata_size > file_size - footer_size) {
        throw error::MakeError("columnar", "metadata size exceeds file size");
    }

    in.seekg(static_cast<std::streamoff>(file_size - footer_size - metadata_size));
    if (!in) {
        throw error::MakeError("columnar", "failed to seek to metadata");
    }

    return ReadMetadata(in);
}
