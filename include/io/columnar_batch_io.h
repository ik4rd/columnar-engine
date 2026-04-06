/* (What is this? — Чтение/запись батчей из CSV в колоночный формат) */

#pragma once

#include <filesystem>
#include <fstream>
#include <optional>

#include "batch_io.h"
#include "fileio.h"
#include "metadata.h"

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
    std::ifstream in_;
    ColumnarMetadata metadata_;
    size_t next_group_ = 0;
};

class ColumnarBatchWriter final : public BatchWriter {
   public:
    ColumnarBatchWriter(const std::filesystem::path& path, Schema schema);
    ColumnarBatchWriter(const ColumnarBatchWriter&) = delete;
    ColumnarBatchWriter(ColumnarBatchWriter&&) noexcept = default;
    ColumnarBatchWriter& operator=(const ColumnarBatchWriter&) = delete;
    ColumnarBatchWriter& operator=(ColumnarBatchWriter&&) noexcept = default;
    ~ColumnarBatchWriter() override = default;

    void Write(const Batch& batch) override;
    void Flush() override;

    void Finalize();

    const ColumnarMetadata& GetMetadata() const { return metadata_; }

   private:
    std::filesystem::path path_;
    std::ofstream out_;
    ColumnarMetadata metadata_;
    bool finalized_ = false;
};
