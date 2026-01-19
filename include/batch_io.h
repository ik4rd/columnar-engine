#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "batch.h"
#include "fileio.h"
#include "metadata.h"

struct BatchSizing {
    std::optional<size_t> max_rows;
    std::optional<size_t> max_values;
    std::optional<uint64_t> max_bytes;

    bool WouldExceed(size_t next_rows, size_t column_count, uint64_t next_bytes) const;
};

class BatchReader {
   public:
    virtual ~BatchReader() = default;
    virtual std::optional<Batch> ReadNext() = 0;
};

class BatchWriter {
   public:
    virtual ~BatchWriter() = default;
    virtual void Write(const Batch& batch) = 0;
    virtual void Flush() = 0;
};

class CsvBatchReader final : public BatchReader {
   public:
    CsvBatchReader(const std::filesystem::path& path, Schema schema, BatchSizing sizing);

    std::optional<Batch> ReadNext() override;

    const Schema& GetSchema() const { return schema_; }

   private:
    FileReader reader_;
    Schema schema_;
    BatchSizing sizing_;
    std::optional<std::vector<std::string>> pending_row_;
    bool reached_eof_ = false;
};

class CsvBatchWriter final : public BatchWriter {
   public:
    CsvBatchWriter(const std::filesystem::path& path, Schema schema);

    void Write(const Batch& batch) override;
    void Flush() override;

   private:
    FileWriter writer_;
    Schema schema_;
};

class ColumnarBatchReader final : public BatchReader {
   public:
    explicit ColumnarBatchReader(const std::filesystem::path& path);

    std::optional<Batch> ReadNext() override;

    const Schema& GetSchema() const { return metadata_.schema; }
    const ColumnarMetadata& GetMetadata() const { return metadata_; }

   private:
    FileReader reader_;
    ColumnarMetadata metadata_;
    size_t next_group_ = 0;
};

class ColumnarBatchWriter final : public BatchWriter {
   public:
    ColumnarBatchWriter(const std::filesystem::path& path, Schema schema);

    void Write(const Batch& batch) override;
    void Flush() override;

    void Finalize();

    const ColumnarMetadata& GetMetadata() const { return metadata_; }

   private:
    FileWriter writer_;
    ColumnarMetadata metadata_;
    bool finalized_ = false;
};
