#include "columnar.h"

#include <limits>

#include "csv.h"
#include "error.h"
#include "fileio.h"
#include "magic.h"
#include "metadata.h"
#include "parsing.h"
#include "row_group.h"
#include "stream.h"

Schema ReadSchemaCsv(const std::filesystem::path& path) {
    FileReader reader(path);
    auto& in = reader.Stream();

    Schema schema;
    std::vector<std::string> row;

    while (ReadCsvRow(in, row)) {
        if (row.size() == 1 && row[0].empty()) {
            continue;
        }
        if (row.size() != 2) {
            throw error::MakeError("columnar", "schema csv must have 2 columns");
        }
        schema.columns.push_back(ColumnSchema{
            row[0],
            ParseColumnType(row[1]),
        });
    }

    if (schema.columns.empty()) {
        throw error::MakeError("columnar", "schema csv is empty");
    }

    return schema;
}

void WriteSchemaCsv(const std::filesystem::path& path, const Schema& schema) {
    FileWriter writer(path);
    auto& out = writer.Stream();

    for (const auto& [name, type] : schema.columns) {
        WriteCsvRow(out, {name, ColumnTypeToString(type)});
    }

    writer.Flush();
}

void ConvertCsvToColumnar(const std::filesystem::path& schema_path, const std::filesystem::path& data_path,
                          const std::filesystem::path& output_path, size_t max_rows_per_group) {
    if (max_rows_per_group == 0) {
        throw error::MakeError("columnar", "row group size must be > 0");
    }

    Schema schema = ReadSchemaCsv(schema_path);

    FileReader reader(data_path);
    auto& in = reader.Stream();

    FileWriter writer(output_path);
    auto& out = writer.Stream();

    ColumnarMetadata metadata;
    metadata.schema = schema;

    std::vector<ColumnBuffer> buffers;
    buffers.reserve(schema.columns.size());

    for (const auto& [name, type] : schema.columns) {
        ColumnBuffer buffer;
        buffer.type = type;
        buffers.push_back(std::move(buffer));
    }

    std::vector<std::string> row;
    size_t row_count = 0;

    while (ReadCsvRow(in, row)) {
        if (row.size() != schema.columns.size()) {
            throw error::MakeError("columnar", "data csv column count mismatch");
        }

        for (size_t col = 0; col < schema.columns.size(); ++col) {
            auto& [type, int_values, string_values] = buffers[col];
            if (type == ColumnType::Int64) {
                int_values.push_back(ParseInt64(row[col]));
            } else {
                string_values.push_back(row[col]);
            }
        }

        ++row_count;

        if (row_count == max_rows_per_group) {
            if (row_count > std::numeric_limits<uint32_t>::max()) {
                throw error::MakeError("columnar", "row group exceeds supported size");
            }
            RowGroupMetadata group;
            FlushRowGroup(writer, buffers, static_cast<uint32_t>(row_count), group);
            metadata.row_groups.push_back(std::move(group));
            row_count = 0;
        }
    }

    if (row_count > 0) {
        if (row_count > std::numeric_limits<uint32_t>::max()) {
            throw error::MakeError("columnar", "row group exceeds supported size");
        }
        RowGroupMetadata group;
        FlushRowGroup(writer, buffers, static_cast<uint32_t>(row_count), group);
        metadata.row_groups.push_back(std::move(group));
    }

    const uint64_t metadata_start = writer.Tell();
    WriteMetadata(out, metadata);
    const uint64_t metadata_end = writer.Tell();
    const uint64_t metadata_size = metadata_end - metadata_start;

    WriteLittleEndian<uint64_t>(out, metadata_size);
    WriteBytes(out, ColumnarMagic());

    writer.Flush();
}

void ConvertColumnarToCsv(const std::filesystem::path& columnar_path, const std::filesystem::path& schema_path,
                          const std::filesystem::path& data_path) {
    ColumnarMetadata metadata = ReadColumnarMetadata(columnar_path);
    WriteSchemaCsv(schema_path, metadata.schema);

    FileReader reader(columnar_path);
    auto& in = reader.Stream();

    FileWriter writer(data_path);
    auto& out = writer.Stream();

    const size_t column_count = metadata.schema.columns.size();
    std::vector<std::string> row(column_count);

    for (const RowGroupMetadata& group : metadata.row_groups) {
        if (group.columns.size() != column_count) {
            throw error::MakeError("columnar", "row group column count mismatch");
        }

        std::vector<ColumnBuffer> columns;
        columns.reserve(column_count);

        for (const auto& [name, type] : metadata.schema.columns) {
            ColumnBuffer buffer;
            buffer.type = type;
            columns.push_back(std::move(buffer));
        }

        for (size_t col = 0; col < column_count; ++col) {
            const auto& [name, type] = metadata.schema.columns[col];
            const auto& [offset, size] = group.columns[col];
            reader.Seek(offset);

            if (type == ColumnType::Int64) {
                const uint64_t expected = static_cast<uint64_t>(group.row_count) * sizeof(int64_t);
                if (size != expected) {
                    throw error::MakeError("columnar", "int64 column chunk size mismatch");
                }
                columns[col].int_values.resize(group.row_count);
                for (uint32_t row_index = 0; row_index < group.row_count; ++row_index) {
                    columns[col].int_values[row_index] = ReadLittleEndian<int64_t>(in);
                }
            } else {
                columns[col].string_values.reserve(group.row_count);
                uint64_t consumed = 0;
                for (uint32_t row_index = 0; row_index < group.row_count; ++row_index) {
                    const uint32_t length = ReadLittleEndian<uint32_t>(in);
                    consumed += sizeof(uint32_t);
                    std::string value(length, '\0');
                    ReadBytes(in, value.data(), value.size());
                    consumed += length;
                    columns[col].string_values.push_back(std::move(value));
                }
                if (consumed != size) {
                    throw error::MakeError("columnar", "string column chunk size mismatch");
                }
            }
        }

        for (uint32_t row_index = 0; row_index < group.row_count; ++row_index) {
            for (size_t col = 0; col < column_count; ++col) {
                const auto& [name, type] = metadata.schema.columns[col];
                if (type == ColumnType::Int64) {
                    row[col] = std::to_string(columns[col].int_values[row_index]);
                } else {
                    row[col] = columns[col].string_values[row_index];
                }
            }
            WriteCsvRow(out, row);
        }
    }

    writer.Flush();
}
