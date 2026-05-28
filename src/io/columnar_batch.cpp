#include "io/columnar_batch.h"

#include <cstdint>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include "common/error.h"
#include "io/stream.h"

static constexpr std::string_view ColumnarMagic = "CLMN";

static uint64_t TellWrite(const std::filesystem::path& path, std::ofstream& out) {
    const auto pos = out.tellp();
    if (pos == -1) {
        throw Error::PathIo("io", path, "tell file");
    }
    return pos;
}

static void FlushWrite(const std::filesystem::path& path, std::ofstream& out) {
    out.flush();
    if (!out) {
        throw Error::PathIo("io", path, "write file");
    }
}

static bool SupportsChunkMinMax(const ColumnType type) { return type != ColumnType::String; }

static void PopulateChunkMinMax(const Column& column, ColumnChunkMetadata& chunk) {
    if (!SupportsChunkMinMax(column.Type()) || column.Size() == 0) {
        return;
    }

    Int128 min_value = column.ValueAsInt128(0);
    Int128 max_value = min_value;

    for (size_t row = 1; row < column.Size(); ++row) {
        const Int128 value = column.ValueAsInt128(row);
        if (value < min_value) {
            min_value = value;
        }
        if (value > max_value) {
            max_value = value;
        }
    }

    chunk.has_min_max = true;
    chunk.min_value = min_value;
    chunk.max_value = max_value;
}

ColumnarMetadata ColumnarBatchReader::ReadFileMetadata(const std::filesystem::path& path) {
    const auto file_metadata = GetFileMetadata(path);

    if (!file_metadata || !file_metadata->is_regular) {
        throw Error::NotFound("io", "columnar file not found", path.string());
    }

    const uint64_t file_size = file_metadata->size;
    constexpr uint64_t FooterSize = sizeof(uint64_t) + ColumnarMagic.size();

    if (file_size < FooterSize) {
        throw Error::MalformedData("io", "columnar file is too small", path.string());
    }

    InputFile file(path);
    const uint64_t metadata_size = file.ReadAt<uint64_t>(file_size - FooterSize);
    const std::string magic_read = file.ReadStringAt(file_size - ColumnarMagic.size(), ColumnarMagic.size());

    if (magic_read != ColumnarMagic) {
        throw Error::MalformedData("io", "invalid columnar magic", path.string());
    }

    if (metadata_size > file_size - FooterSize) {
        throw Error::MalformedData("io", "metadata size exceeds file size", path.string());
    }

    const std::string metadata_blob = file.ReadStringAt(file_size - FooterSize - metadata_size, metadata_size);
    std::istringstream metadata_stream(metadata_blob);

    return ReadMetadata(metadata_stream);
}

ColumnarBatchReader::ColumnarBatchReader(const std::filesystem::path& path)
    : path_(path), input_(path), metadata_(ReadFileMetadata(path)) {
    if (metadata_.schema.columns.empty()) {
        throw Error::MalformedData("io", "columnar schema is empty", path.string());
    }
}

std::optional<Batch> ColumnarBatchReader::ReadNext() {
    if (next_group_ >= metadata_.row_groups.size()) {
        return std::nullopt;
    }

    const auto& [row_count, row_group_columns] = metadata_.row_groups[next_group_++];
    Batch batch(metadata_.schema, row_count);

    if (row_group_columns.size() != batch.ColumnsCount()) {
        throw Error::InconsistentData("io", "row group column count mismatch", path_.string());
    }

    for (size_t col = 0; col < batch.ColumnsCount(); ++col) {
        const auto& chunk = row_group_columns[col];
        batch.ReadColumnFrom(col, input_.StreamAt(chunk.offset), row_count, chunk.size);
    }

    return batch;
}

ColumnarBatchWriter::ColumnarBatchWriter(const std::filesystem::path& path, Schema schema)
    : path_(path), out_(OpenOutputFile(path)) {
    if (schema.columns.empty()) {
        throw Error::InvalidArgument("io", "schema has no columns", path.string());
    }
    metadata_.schema = std::move(schema);
}

void ColumnarBatchWriter::Write(const Batch& batch) {
    if (finalized_) {
        throw Error::InvalidState("io", "writer already finalized", path_.string());
    }

    batch.Validate();
    if (batch.GetSchema() != metadata_.schema) {
        throw Error::InconsistentData("io", "batch schema mismatch", path_.string());
    }

    const size_t row_count = batch.RowsCount();

    if (row_count == 0) {
        return;
    }

    if (row_count > std::numeric_limits<uint32_t>::max()) {
        throw Error::Overflow("io", "row group exceeds supported size", path_.string());
    }

    RowGroupMetadata group;
    group.row_count = row_count;
    group.columns.reserve(batch.ColumnsCount());

    for (size_t column_index = 0; column_index < batch.ColumnsCount(); ++column_index) {
        const uint64_t offset = TellWrite(path_, out_);
        const Column& column = batch.ColumnAt(column_index);
        column.WriteTo(out_);

        const uint64_t end = TellWrite(path_, out_);

        ColumnChunkMetadata chunk{offset, end - offset};
        PopulateChunkMinMax(column, chunk);
        group.columns.push_back(chunk);
    }

    metadata_.row_groups.push_back(std::move(group));
}

void ColumnarBatchWriter::Flush() { FlushWrite(path_, out_); }

void ColumnarBatchWriter::Finalize() && {
    if (finalized_) {
        throw Error::InvalidState("io", "writer already finalized", path_.string());
    }

    const uint64_t metadata_start = TellWrite(path_, out_);
    WriteMetadata(out_, metadata_);
    const uint64_t metadata_end = TellWrite(path_, out_);
    const uint64_t metadata_size = metadata_end - metadata_start;

    WriteStream<uint64_t>(out_, metadata_size);
    WriteBytes(out_, ColumnarMagic);

    FlushWrite(path_, out_);

    finalized_ = true;
}
