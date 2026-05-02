#include <algorithm>
#include <limits>
#include <utility>

#include "columnar_batch_io.h"
#include "csv.h"
#include "csv_batch_io.h"
#include "error.h"
#include "metadata.h"
#include "stream.h"

static constexpr std::string_view kColumnarMagic = "CLMN";

static void SeekRead(const std::filesystem::path& path, std::ifstream& in, const uint64_t pos) {
    in.seekg(pos);
    if (!in) {
        throw Error::PathIo("batch_io", path, "seek file");
    }
}

static uint64_t TellWrite(const std::filesystem::path& path, std::ofstream& out) {
    const auto pos = out.tellp();
    if (pos == -1) {
        throw Error::PathIo("batch_io", path, "tell file");
    }
    return pos;
}

static void FlushWrite(const std::filesystem::path& path, std::ofstream& out) {
    out.flush();
    if (!out) {
        throw Error::PathIo("batch_io", path, "write file");
    }
}

static uint64_t AddChecked(const uint64_t current, const uint64_t add) {
    if (add > std::numeric_limits<uint64_t>::max() - current) {
        throw Error::Overflow("batch_io", "batch byte size overflow");
    }
    return current + add;
}

static uint64_t EstimateRowBytes(const std::vector<std::unique_ptr<Column>>& columns,
                                 const std::vector<std::string>& row) {
    uint64_t bytes = 0;
    for (size_t i = 0; i < columns.size(); ++i) {
        bytes = AddChecked(bytes, columns[i]->EstimateSizeFromString(row[i]));
    }
    return bytes;
}

static void ValidateSizing(const BatchSizing& sizing) {
    if (sizing.max_rows && *sizing.max_rows == 0) {
        throw Error::InvalidArgument("batch_io", "max rows must be > 0");
    }
    if (sizing.max_values && *sizing.max_values == 0) {
        throw Error::InvalidArgument("batch_io", "max values must be > 0");
    }
    if (sizing.max_bytes && *sizing.max_bytes == 0) {
        throw Error::InvalidArgument("batch_io", "max bytes must be > 0");
    }
}

static void ReserveForSizing(const Batch& batch, const BatchSizing& sizing, const size_t column_count) {
    if (sizing.max_rows) {
        batch.Reserve(*sizing.max_rows);
        return;
    }
    if (sizing.max_values && column_count > 0) {
        batch.Reserve(std::max<size_t>(1, *sizing.max_values / column_count));
    }
}

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
    : csv_reader_(path), schema_(std::move(schema)), sizing_(std::move(sizing)) {
    if (schema_.columns.empty()) {
        throw Error::InvalidArgument("batch_io", "schema has no columns");
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
    const auto& columns = batch.GetColumns();

    std::vector<std::string> row;
    row.reserve(column_count);

    size_t rows = 0;
    uint64_t bytes = 0;

    while (true) {
        if (pending_row_) {
            row = std::move(*pending_row_);
            pending_row_.reset();
        } else {
            if (!csv_reader_.ReadRow(row)) {
                reached_eof_ = true;
                break;
            }
        }

        if (row.size() != column_count) {
            throw Error::Mismatch("batch_io", "data csv column count mismatch");
        }

        const size_t next_rows = rows + 1;
        uint64_t next_bytes = bytes;

        if (sizing_.max_bytes) {
            next_bytes = AddChecked(bytes, EstimateRowBytes(columns, row));
        }

        if (rows > 0 && sizing_.WouldExceed(next_rows, column_count, next_bytes)) {
            pending_row_ = std::move(row);
            break;
        }

        for (size_t col = 0; col < column_count; ++col) {
            columns[col]->AppendFromString(row[col]);
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
    : csv_writer_(path), schema_(std::move(schema)) {
    if (schema_.columns.empty()) {
        throw Error::InvalidArgument("batch_io", "schema has no columns");
    }
}

void CsvBatchWriter::Write(const Batch& batch) {
    batch.Validate();
    if (batch.GetSchema() != schema_) {
        throw Error::Mismatch("batch_io", "batch schema mismatch");
    }

    const size_t column_count = schema_.columns.size();
    const size_t row_count = batch.RowsCount();

    std::vector<std::string> row(column_count);

    for (size_t row_index = 0; row_index < row_count; ++row_index) {
        for (size_t col = 0; col < column_count; ++col) {
            row[col] = batch.ColumnAt(col).ValueAsString(row_index);
        }
        csv_writer_.WriteRow(row);
    }
}

void CsvBatchWriter::Flush() { csv_writer_.Flush(); }

void AppendBatchRows(const Batch& batch, std::vector<std::vector<std::string>>& rows) {
    const size_t row_count = batch.RowsCount();
    const size_t column_count = batch.ColumnsCount();

    for (size_t row = 0; row < row_count; ++row) {
        std::vector<std::string> values;
        values.reserve(column_count);

        for (size_t col = 0; col < column_count; ++col) {
            values.push_back(batch.ColumnAt(col).ValueAsString(row));
        }

        rows.push_back(std::move(values));
    }
}

void WriteBatchCsv(const std::filesystem::path& path, const Batch& batch) {
    CsvBatchWriter writer(path, batch.GetSchema());
    writer.Write(batch);
    writer.Flush();
}

ColumnarMetadata ColumnarBatchReader::ReadFileMetadata(const std::filesystem::path& path) {
    const auto file_metadata = TryGetFileMetadata(path);

    if (!file_metadata || !file_metadata->is_regular) {
        throw Error::NotFound("batch_io", "columnar file not found", path.string());
    }

    const uint64_t file_size = file_metadata->size;
    constexpr uint64_t kFooterSize = sizeof(uint64_t) + kColumnarMagic.size();

    if (file_size < kFooterSize) {
        throw Error::InvalidData("batch_io", "columnar file is too small", path.string());
    }

    std::ifstream in = OpenInputFile(path);
    SeekRead(path, in, file_size - kFooterSize);

    const uint64_t metadata_size = ReadStream<uint64_t>(in);
    std::string magic_read(kColumnarMagic.size(), '\0');
    ReadBytes(in, magic_read.data(), magic_read.size());

    if (magic_read != kColumnarMagic) {
        throw Error::InvalidData("batch_io", "invalid columnar magic", path.string());
    }

    if (metadata_size > file_size - kFooterSize) {
        throw Error::InvalidData("batch_io", "metadata size exceeds file size", path.string());
    }

    SeekRead(path, in, file_size - kFooterSize - metadata_size);

    return ReadMetadata(in);
}

ColumnarBatchReader::ColumnarBatchReader(const std::filesystem::path& path)
    : path_(path), in_(OpenInputFile(path)), metadata_(ReadFileMetadata(path)) {
    if (metadata_.schema.columns.empty()) {
        throw Error::InvalidData("batch_io", "columnar schema is empty", path.string());
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
        throw Error::Mismatch("batch_io", "row group column count mismatch", path_.string());
    }

    for (size_t col = 0; col < columns.size(); ++col) {
        const auto& [offset, size] = row_group_columns[col];
        SeekRead(path_, in_, offset);
        columns[col]->ReadFrom(in_, row_count, size);
    }

    return batch;
}

ColumnarBatchWriter::ColumnarBatchWriter(const std::filesystem::path& path, Schema schema)
    : path_(path), out_(OpenOutputFile(path)) {
    if (schema.columns.empty()) {
        throw Error::InvalidArgument("batch_io", "schema has no columns", path.string());
    }
    metadata_.schema = std::move(schema);
}

void ColumnarBatchWriter::Write(const Batch& batch) {
    if (finalized_) {
        throw Error::InvalidState("batch_io", "writer already finalized", path_.string());
    }

    batch.Validate();
    if (batch.GetSchema() != metadata_.schema) {
        throw Error::Mismatch("batch_io", "batch schema mismatch", path_.string());
    }

    const size_t row_count = batch.RowsCount();

    if (row_count == 0) {
        return;
    }
    if (row_count > std::numeric_limits<uint32_t>::max()) {
        throw Error::Overflow("batch_io", "row group exceeds supported size", path_.string());
    }

    RowGroupMetadata group;
    group.row_count = row_count;
    group.columns.reserve(batch.ColumnsCount());

    for (const auto& column : batch.GetColumns()) {
        const uint64_t offset = TellWrite(path_, out_);
        column->WriteTo(out_);
        const uint64_t end = TellWrite(path_, out_);
        group.columns.push_back(ColumnChunkMetadata{offset, end - offset});
    }

    metadata_.row_groups.push_back(std::move(group));
}

void ColumnarBatchWriter::Flush() { FlushWrite(path_, out_); }

void ColumnarBatchWriter::Finalize() {
    if (finalized_) {
        throw Error::InvalidState("batch_io", "writer already finalized", path_.string());
    }

    const uint64_t metadata_start = TellWrite(path_, out_);
    WriteMetadata(out_, metadata_);
    const uint64_t metadata_end = TellWrite(path_, out_);
    const uint64_t metadata_size = metadata_end - metadata_start;

    WriteStream<uint64_t>(out_, metadata_size);
    WriteBytes(out_, kColumnarMagic);

    FlushWrite(path_, out_);
    finalized_ = true;
}
