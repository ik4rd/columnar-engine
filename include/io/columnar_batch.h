#pragma once

#include <filesystem>
#include <optional>
#include <span>
#include <vector>

#include "io/batch.h"
#include "io/compression.h"
#include "io/file.h"
#include "model/metadata.h"

class ColumnarBatchReader final : public BatchReader {
   public:
    explicit ColumnarBatchReader(const std::filesystem::path& path);
    ColumnarBatchReader(const ColumnarBatchReader&) = delete;
    ColumnarBatchReader(ColumnarBatchReader&&) noexcept = default;
    ColumnarBatchReader& operator=(const ColumnarBatchReader&) = delete;
    ColumnarBatchReader& operator=(ColumnarBatchReader&&) noexcept = default;
    ~ColumnarBatchReader() override = default;

    std::optional<Batch> ReadNext() override;

    const Schema& GetSchema() const { return metadata_.schema; }
    const ColumnarMetadata& GetMetadata() const { return metadata_; }

   private:
    static ColumnarMetadata ReadFileMetadata(const std::filesystem::path& path);

    std::filesystem::path path_;
    InputFile input_;

    ColumnarMetadata metadata_;
    size_t next_group_ = 0;
};

class ColumnarBatchWriter final : public BatchWriter {
   public:
    ColumnarBatchWriter(const std::filesystem::path& path, Schema schema, Compression compression = Compression::None);
    ColumnarBatchWriter(const ColumnarBatchWriter&) = delete;
    ColumnarBatchWriter(ColumnarBatchWriter&&) noexcept = default;
    ColumnarBatchWriter& operator=(const ColumnarBatchWriter&) = delete;
    ColumnarBatchWriter& operator=(ColumnarBatchWriter&&) noexcept = default;
    ~ColumnarBatchWriter() override = default;

    void Write(const Batch& batch) override;
    void Flush() override;

    void Finalize() &;
    void Finalize() &&;

    const ColumnarMetadata& GetMetadata() const { return metadata_; }

   private:
    std::filesystem::path path_;
    std::ofstream out_;

    ColumnarMetadata metadata_;
    Compression compression_ = Compression::None;
    bool finalized_ = false;
};

std::vector<uint8_t> ReadColumnChunk(const std::filesystem::path& path, InputFile& input,
                                     const ColumnChunkMetadata& chunk);
void ReadBatchColumnChunk(const std::filesystem::path& path, InputFile& input, const ColumnChunkMetadata& chunk,
                          uint32_t row_count, const Batch& batch, size_t column_index);
