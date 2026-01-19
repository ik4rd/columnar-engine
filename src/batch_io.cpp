#include "batch_io.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "csv.h"
#include "error.h"
#include "magic.h"
#include "metadata.h"
#include "stream.h"
#include "utils.h"

namespace {
void ValidateSizing(const BatchSizing& sizing) {
    if (sizing.max_rows && *sizing.max_rows == 0) {
        throw error::MakeError("batch_io", "max rows must be > 0");
    }
    if (sizing.max_values && *sizing.max_values == 0) {
        throw error::MakeError("batch_io", "max values must be > 0");
    }
    if (sizing.max_bytes && *sizing.max_bytes == 0) {
        throw error::MakeError("batch_io", "max bytes must be > 0");
    }
}

void ReserveForSizing(const Batch& batch, const BatchSizing& sizing, const size_t column_count) {
    if (sizing.max_rows) {
        batch.Reserve(*sizing.max_rows);
        return;
    }
    if (sizing.max_values && column_count > 0) {
        batch.Reserve(std::max<size_t>(1, *sizing.max_values / column_count));
    }
}
}  // namespace

bool BatchSizing::WouldExceed(const size_t next_rows, const size_t column_count, const uint64_t next_bytes) const {
    if (max_rows && next_rows > *max_rows) {
        return true;
    }

    if (max_values && column_count > 0) {
        if (next_rows > *max_values / column_count) {
            return true;
        }
    }

    if (max_bytes && next_bytes > *max_bytes) {
        return true;
    }

    return false;
}

CsvBatchReader::CsvBatchReader(const std::filesystem::path& path, Schema schema, BatchSizing sizing)
    : reader_(path), schema_(std::move(schema)), sizing_(std::move(sizing)) {
    if (schema_.columns.empty()) {
        throw error::MakeError("batch_io", "schema has no columns");
    }
    ValidateSizing(sizing_);
}

std::optional<Batch> CsvBatchReader::ReadNext() {
    if (reached_eof_ && !pending_row_) {
        return std::nullopt;
    }

    Batch batch(schema_);
    const size_t column_count = schema_.columns.size();
    ReserveForSizing(batch, sizing_, column_count);

    auto& in = reader_.Stream();
    std::vector<std::string> row;
    size_t rows = 0;
    uint64_t bytes = 0;

    while (true) {
        if (pending_row_) {
            row = std::move(*pending_row_);
            pending_row_.reset();
        } else {
            if (!ReadCsvRow(in, row)) {
                reached_eof_ = true;
                break;
            }
        }

        if (row.size() != column_count) {
            throw error::MakeError("batch_io", "data csv column count mismatch");
        }

        const uint64_t row_bytes = EstimateRowBytes(schema_, row);
        const size_t next_rows = rows + 1;
        const uint64_t next_bytes = AddBytesChecked(bytes, row_bytes);

        if (rows > 0 && sizing_.WouldExceed(next_rows, column_count, next_bytes)) {
            pending_row_ = std::move(row);
            break;
        }

        for (size_t col = 0; col < column_count; ++col) {
            batch.ColumnAt(col).AppendFromString(row[col]);
        }

        rows = next_rows;
        bytes = next_bytes;
    }

    if (rows == 0 && reached_eof_ && !pending_row_) {
        return std::nullopt;
    }

    return batch;
}

CsvBatchWriter::CsvBatchWriter(const std::filesystem::path& path, Schema schema)
    : writer_(path), schema_(std::move(schema)) {
    if (schema_.columns.empty()) {
        throw error::MakeError("batch_io", "schema has no columns");
    }
}

void CsvBatchWriter::Write(const Batch& batch) {
    batch.Validate();
    if (!SchemasEqual(batch.GetSchema(), schema_)) {
        throw error::MakeError("batch_io", "batch schema mismatch");
    }

    auto& out = writer_.Stream();
    const size_t column_count = schema_.columns.size();
    const size_t row_count = batch.RowsCount();
    std::vector<std::string> row(column_count);

    for (size_t row_index = 0; row_index < row_count; ++row_index) {
        for (size_t col = 0; col < column_count; ++col) {
            row[col] = batch.ColumnAt(col).ValueAsString(row_index);
        }
        WriteCsvRow(out, row);
    }
}

void CsvBatchWriter::Flush() { writer_.Flush(); }

ColumnarBatchReader::ColumnarBatchReader(const std::filesystem::path& path)
    : reader_(path), metadata_(ReadColumnarMetadata(path)) {
    if (metadata_.schema.columns.empty()) {
        throw error::MakeError("batch_io", "columnar schema is empty");
    }
}

std::optional<Batch> ColumnarBatchReader::ReadNext() {
    if (next_group_ >= metadata_.row_groups.size()) {
        return std::nullopt;
    }

    const auto& [row_count, row_group_columns] = metadata_.row_groups[next_group_++];
    Batch batch(metadata_.schema, row_count);

    const auto& columns = batch.GetColumns();
    if (row_group_columns.size() != columns.size()) {
        throw error::MakeError("batch_io", "row group column count mismatch");
    }

    auto& in = reader_.Stream();
    for (size_t col = 0; col < columns.size(); ++col) {
        const auto& [offset, size] = row_group_columns[col];
        reader_.Seek(offset);
        columns[col]->ReadFrom(in, row_count, size);
    }

    return batch;
}

ColumnarBatchWriter::ColumnarBatchWriter(const std::filesystem::path& path, Schema schema) : writer_(path) {
    if (schema.columns.empty()) {
        throw error::MakeError("batch_io", "schema has no columns");
    }
    metadata_.schema = std::move(schema);
}

void ColumnarBatchWriter::Write(const Batch& batch) {
    if (finalized_) {
        throw error::MakeError("batch_io", "writer already finalized");
    }

    batch.Validate();
    if (!SchemasEqual(batch.GetSchema(), metadata_.schema)) {
        throw error::MakeError("batch_io", "batch schema mismatch");
    }

    const size_t row_count = batch.RowsCount();
    if (row_count == 0) {
        return;
    }
    if (row_count > std::numeric_limits<uint32_t>::max()) {
        throw error::MakeError("batch_io", "row group exceeds supported size");
    }

    RowGroupMetadata group;
    group.row_count = static_cast<uint32_t>(row_count);
    group.columns.reserve(batch.ColumnsCount());

    auto& out = writer_.Stream();
    for (const auto& column : batch.GetColumns()) {
        const uint64_t offset = writer_.Tell();
        column->WriteTo(out);
        const uint64_t end = writer_.Tell();
        group.columns.push_back(ColumnChunkMetadata{offset, end - offset});
    }

    metadata_.row_groups.push_back(std::move(group));
}

void ColumnarBatchWriter::Flush() { writer_.Flush(); }

void ColumnarBatchWriter::Finalize() {
    if (finalized_) {
        throw error::MakeError("batch_io", "writer already finalized");
    }

    auto& out = writer_.Stream();
    const uint64_t metadata_start = writer_.Tell();
    WriteMetadata(out, metadata_);
    const uint64_t metadata_end = writer_.Tell();
    const uint64_t metadata_size = metadata_end - metadata_start;

    WriteLittleEndian<uint64_t>(out, metadata_size);
    WriteBytes(out, ColumnarMagic());

    writer_.Flush();
    finalized_ = true;
}
