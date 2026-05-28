#include "model/metadata.h"

#include <array>
#include <limits>
#include <sstream>
#include <string>

#include "common/error.h"
#include "io/stream.h"

static constexpr std::array<char, 4> MetadataMagic = {'C', 'M', 'D', '2'};

static ColumnType ColumnTypeFromByte(const uint8_t type_byte) {
    switch (static_cast<ColumnType>(type_byte)) {
        case ColumnType::Int64:
        case ColumnType::String:
        case ColumnType::Boolean:
        case ColumnType::Int16:
        case ColumnType::Int32:
        case ColumnType::Int128:
        case ColumnType::Date:
        case ColumnType::Timestamp:
        case ColumnType::Character:
            return static_cast<ColumnType>(type_byte);
    }

    throw Error::MalformedData("model", "unknown column type in metadata");
}

static Compression CompressionFromByte(const uint8_t compression_byte) {
    switch (static_cast<Compression>(compression_byte)) {
        case Compression::None:
        case Compression::Lz4:
            return static_cast<Compression>(compression_byte);
    }

    throw Error::MalformedData("model", "unknown compression codec in metadata");
}

static ColumnarMetadata ReadLegacyMetadata(std::istream& in) {
    ColumnarMetadata metadata;

    const uint32_t column_count = ReadStream<uint32_t>(in);
    metadata.schema.columns.reserve(column_count);

    for (uint32_t i = 0; i < column_count; ++i) {
        const uint32_t name_size = ReadStream<uint32_t>(in);
        std::string name(name_size, '\0');

        ReadBytes(in, name.data(), name.size());

        const uint8_t type_byte = ReadStream<uint8_t>(in);
        const ColumnType type = ColumnTypeFromByte(type_byte);

        metadata.schema.columns.push_back(ColumnSchema{std::move(name), type});
    }

    const uint32_t row_group_count = ReadStream<uint32_t>(in);
    metadata.row_groups.reserve(row_group_count);

    for (uint32_t i = 0; i < row_group_count; ++i) {
        RowGroupMetadata group;
        group.row_count = ReadStream<uint32_t>(in);

        const uint32_t columns_in_group = ReadStream<uint32_t>(in);
        if (columns_in_group != column_count) {
            throw Error::InconsistentData("model", "row group column count mismatch");
        }

        group.columns.reserve(columns_in_group);

        for (uint32_t col = 0; col < columns_in_group; ++col) {
            ColumnChunkMetadata chunk;
            chunk.offset = ReadStream<uint64_t>(in);
            chunk.compressed_size = ReadStream<uint64_t>(in);
            chunk.uncompressed_size = chunk.compressed_size;
            chunk.compression = Compression::None;

            group.columns.push_back(chunk);
        }

        metadata.row_groups.push_back(std::move(group));
    }

    if (in.peek() == std::istream::traits_type::eof()) {
        return metadata;
    }

    for (auto& row_group : metadata.row_groups) {
        for (auto& column : row_group.columns) {
            column.has_min_max = ReadStream<uint8_t>(in) != 0;
            if (!column.has_min_max) {
                continue;
            }

            column.min_value = ReadStream<Int128>(in);
            column.max_value = ReadStream<Int128>(in);
        }
    }

    return metadata;
}

static ColumnarMetadata ReadV2Metadata(std::istream& in) {
    ColumnarMetadata metadata;

    const uint32_t version = ReadStream<uint32_t>(in);
    if (version != 2) {
        throw Error::MalformedData("model", "unsupported metadata version");
    }

    const uint32_t column_count = ReadStream<uint32_t>(in);
    metadata.schema.columns.reserve(column_count);

    for (uint32_t i = 0; i < column_count; ++i) {
        const uint32_t name_size = ReadStream<uint32_t>(in);
        std::string name(name_size, '\0');

        ReadBytes(in, name.data(), name.size());

        const uint8_t type_byte = ReadStream<uint8_t>(in);
        const ColumnType type = ColumnTypeFromByte(type_byte);

        metadata.schema.columns.push_back(ColumnSchema{std::move(name), type});
    }

    const uint32_t row_group_count = ReadStream<uint32_t>(in);
    metadata.row_groups.reserve(row_group_count);

    for (uint32_t i = 0; i < row_group_count; ++i) {
        RowGroupMetadata group;
        group.row_count = ReadStream<uint32_t>(in);

        const uint32_t columns_in_group = ReadStream<uint32_t>(in);
        if (columns_in_group != column_count) {
            throw Error::InconsistentData("model", "row group column count mismatch");
        }

        group.columns.reserve(columns_in_group);

        for (uint32_t col = 0; col < columns_in_group; ++col) {
            ColumnChunkMetadata chunk;
            chunk.offset = ReadStream<uint64_t>(in);
            chunk.compressed_size = ReadStream<uint64_t>(in);
            chunk.uncompressed_size = ReadStream<uint64_t>(in);
            chunk.compression = CompressionFromByte(ReadStream<uint8_t>(in));

            group.columns.push_back(chunk);
        }

        metadata.row_groups.push_back(std::move(group));
    }

    for (auto& row_group : metadata.row_groups) {
        for (auto& column : row_group.columns) {
            column.has_min_max = ReadStream<uint8_t>(in) != 0;
            if (!column.has_min_max) {
                continue;
            }

            column.min_value = ReadStream<Int128>(in);
            column.max_value = ReadStream<Int128>(in);
        }
    }

    if (in.peek() != std::istream::traits_type::eof()) {
        throw Error::MalformedData("model", "unexpected trailing bytes in metadata");
    }

    return metadata;
}

ColumnarMetadata ReadMetadata(std::istream& in) {
    const std::streampos start = in.tellg();
    if (start != std::streampos(-1)) {
        char magic[MetadataMagic.size()];
        in.read(magic, sizeof(magic));
        if (in.gcount() == static_cast<std::streamsize>(MetadataMagic.size()) &&
            std::equal(std::begin(magic), std::end(magic), MetadataMagic.begin())) {
            return ReadV2Metadata(in);
        }

        in.clear();
        in.seekg(start);
    }

    return ReadLegacyMetadata(in);
}

void WriteMetadata(std::ostream& out, const ColumnarMetadata& metadata) {
    if (metadata.schema.columns.size() > std::numeric_limits<uint32_t>::max()) {
        throw Error::Overflow("model", "too many columns for metadata");
    }

    WriteBytes(out, {MetadataMagic.data(), MetadataMagic.size()});
    WriteStream<uint32_t>(out, 2);
    WriteStream<uint32_t>(out, metadata.schema.columns.size());

    for (const auto& [name, type] : metadata.schema.columns) {
        if (name.size() > std::numeric_limits<uint32_t>::max()) {
            throw Error::Overflow("model", "column name too long");
        }

        WriteStream<uint32_t>(out, name.size());
        WriteBytes(out, name);
        WriteStream<uint8_t>(out, static_cast<uint8_t>(type));
    }

    if (metadata.row_groups.size() > std::numeric_limits<uint32_t>::max()) {
        throw Error::Overflow("model", "too many row groups for metadata");
    }

    WriteStream<uint32_t>(out, metadata.row_groups.size());

    for (const auto& [row_count, columns] : metadata.row_groups) {
        WriteStream<uint32_t>(out, row_count);
        WriteStream<uint32_t>(out, columns.size());

        for (const auto& column : columns) {
            WriteStream<uint64_t>(out, column.offset);
            WriteStream<uint64_t>(out, column.compressed_size);
            WriteStream<uint64_t>(out, column.uncompressed_size);
            WriteStream<uint8_t>(out, static_cast<uint8_t>(column.compression));
        }
    }

    for (const auto& row_group : metadata.row_groups) {
        for (const auto& column : row_group.columns) {
            WriteStream<uint8_t>(out, column.has_min_max ? 1 : 0);
            if (!column.has_min_max) {
                continue;
            }

            WriteStream(out, column.min_value);
            WriteStream(out, column.max_value);
        }
    }
}
