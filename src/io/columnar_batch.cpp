#include <cstdint>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "io/columnar_batch.h"
#include "io/stream.h"
#include "support/error.h"

static constexpr std::string_view ColumnarMagic = "CLMN";

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

ColumnarMetadata ColumnarBatchReader::ReadFileMetadata(const std::filesystem::path& path) {
    const auto file_metadata = TryGetFileMetadata(path);

    if (!file_metadata || !file_metadata->is_regular) {
        throw Error::NotFound("batch_io", "columnar file not found", path.string());
    }

    const uint64_t file_size = file_metadata->size;
    constexpr uint64_t FooterSize = sizeof(uint64_t) + ColumnarMagic.size();

    if (file_size < FooterSize) {
        throw Error::InvalidData("batch_io", "columnar file is too small", path.string());
    }

    InputFile file(path);
    const uint64_t metadata_size = file.ReadAt<uint64_t>(file_size - FooterSize);
    const std::string magic_read = file.ReadStringAt(file_size - ColumnarMagic.size(), ColumnarMagic.size());

    if (magic_read != ColumnarMagic) {
        throw Error::InvalidData("batch_io", "invalid columnar magic", path.string());
    }

    if (metadata_size > file_size - FooterSize) {
        throw Error::InvalidData("batch_io", "metadata size exceeds file size", path.string());
    }

    return ReadMetadata(file.StreamAt(file_size - FooterSize - metadata_size));
}

ColumnarBatchReader::ColumnarBatchReader(const std::filesystem::path& path)
    : path_(path), input_(path), metadata_(ReadFileMetadata(path)) {
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
        columns[col]->ReadFrom(input_.StreamAt(offset), row_count, size);
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

void ColumnarBatchWriter::Finalize() && {
    if (finalized_) {
        throw Error::InvalidState("batch_io", "writer already finalized", path_.string());
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
