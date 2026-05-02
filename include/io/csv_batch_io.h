/* (What is this? — Чтение/запись CSV) */

#pragma once

#include <filesystem>
#include <optional>
#include <vector>

#include "batch_io.h"
#include "csv.h"

class CsvBatchReader final : public BatchReader {
   public:
    CsvBatchReader(const std::filesystem::path& path, Schema schema, BatchSizing sizing);
    CsvBatchReader(const CsvBatchReader&) = delete;
    CsvBatchReader(CsvBatchReader&&) noexcept = default;
    CsvBatchReader& operator=(const CsvBatchReader&) = delete;
    CsvBatchReader& operator=(CsvBatchReader&&) noexcept = default;
    ~CsvBatchReader() override = default;

   public:
    std::optional<Batch> ReadNext() override;

    const Schema& GetSchema() const { return schema_; }

   private:
    CsvReader csv_reader_;
    Schema schema_;
    BatchSizing sizing_;

    std::optional<std::vector<std::string>> pending_row_;
    bool reached_eof_ = false;
};

class CsvBatchWriter final : public BatchWriter {
   public:
    CsvBatchWriter(const std::filesystem::path& path, Schema schema);
    CsvBatchWriter(const CsvBatchWriter&) = delete;
    CsvBatchWriter(CsvBatchWriter&&) noexcept = default;
    CsvBatchWriter& operator=(const CsvBatchWriter&) = delete;
    CsvBatchWriter& operator=(CsvBatchWriter&&) noexcept = default;
    ~CsvBatchWriter() override = default;

   public:
    void Write(const Batch& batch) override;
    void Flush() override;

   private:
    CsvWriter csv_writer_;
    Schema schema_;
};

void AppendBatchRows(const Batch& batch, std::vector<std::vector<std::string>>& rows);
void WriteBatchCsv(const std::filesystem::path& path, const Batch& batch);
