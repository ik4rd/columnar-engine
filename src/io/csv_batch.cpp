#include "io/csv_batch.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "common/error.h"
#include "common/int128.h"

static uint64_t AddChecked(const uint64_t current, const uint64_t add) {
    if (add > std::numeric_limits<uint64_t>::max() - current) {
        throw Error::Overflow("io", "batch byte size overflow");
    }
    return current + add;
}

static uint64_t EstimateValueBytes(const ColumnType type, const std::string_view value) {
    switch (type) {
        case ColumnType::Boolean:
            return sizeof(uint8_t);
        case ColumnType::Int16:
            return sizeof(int16_t);
        case ColumnType::Int32:
        case ColumnType::Date:
            return sizeof(int32_t);
        case ColumnType::Int64:
        case ColumnType::Timestamp:
            return sizeof(int64_t);
        case ColumnType::Int128:
            return sizeof(Int128);
        case ColumnType::Character:
            return sizeof(char);
        case ColumnType::String:
            return value.size();
    }
    throw Error::Unsupported("io", "unsupported column type");
}

static uint64_t EstimateRowBytes(const Schema& schema, const std::vector<std::string>& row) {
    uint64_t bytes = 0;
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        bytes = AddChecked(bytes, EstimateValueBytes(schema.columns[i].type, row[i]));
    }
    return bytes;
}

static void ValidateSizing(const BatchSizing& sizing) {
    if (sizing.max_rows && *sizing.max_rows == 0) {
        throw Error::InvalidArgument("io", "max rows must be > 0");
    }

    if (sizing.max_values && *sizing.max_values == 0) {
        throw Error::InvalidArgument("io", "max values must be > 0");
    }

    if (sizing.max_bytes && *sizing.max_bytes == 0) {
        throw Error::InvalidArgument("io", "max bytes must be > 0");
    }
}

static void ReserveForSizing(Batch& batch, const BatchSizing& sizing, const size_t column_count) {
    if (sizing.max_rows) {
        batch.Reserve(*sizing.max_rows);
        return;
    }

    if (sizing.max_values && column_count > 0) {
        batch.Reserve(std::max<size_t>(1, *sizing.max_values / column_count));
    }
}

CsvBatchReader::CsvBatchReader(const std::filesystem::path& path, Schema schema, BatchSizing sizing)
    : csv_reader_(path), schema_(std::move(schema)), sizing_(std::move(sizing)) {
    if (schema_.columns.empty()) {
        throw Error::InvalidArgument("io", "schema has no columns");
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
            throw Error::InconsistentData("io", "data csv column count mismatch");
        }

        const size_t next_rows = rows + 1;
        uint64_t next_bytes = bytes;

        if (sizing_.max_bytes) {
            next_bytes = AddChecked(bytes, EstimateRowBytes(schema_, row));
        }

        if (rows > 0 && sizing_.WouldExceed(next_rows, column_count, next_bytes)) {
            pending_row_ = std::move(row);
            break;
        }

        for (size_t col = 0; col < column_count; ++col) {
            batch.AppendValueFromString(col, row[col]);
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
        throw Error::InvalidArgument("io", "schema has no columns");
    }
}

void CsvBatchWriter::Write(const Batch& batch) {
    batch.Validate();
    if (batch.GetSchema() != schema_) {
        throw Error::InconsistentData("io", "batch schema mismatch");
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
