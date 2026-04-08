/* (What is this? — Потоковое чтение/запись батчей из CSV в колоночный формат) */

#pragma once

#include <optional>

#include "batch.h"

struct BatchSizing {
    std::optional<size_t> max_rows;
    std::optional<size_t> max_values;
    std::optional<uint64_t> max_bytes;

    bool WouldExceed(size_t next_rows, size_t column_count, uint64_t next_bytes) const;
};

class BatchReader {
   public:
    BatchReader() = default;
    BatchReader(const BatchReader&) = default;
    BatchReader(BatchReader&&) noexcept = default;
    BatchReader& operator=(const BatchReader&) = default;
    BatchReader& operator=(BatchReader&&) noexcept = default;
    virtual ~BatchReader() = default;

   public:
    virtual std::optional<Batch> ReadNext() = 0;
};

class BatchWriter {
   public:
    BatchWriter() = default;
    BatchWriter(const BatchWriter&) = default;
    BatchWriter(BatchWriter&&) noexcept = default;
    BatchWriter& operator=(const BatchWriter&) = default;
    BatchWriter& operator=(BatchWriter&&) noexcept = default;
    virtual ~BatchWriter() = default;

   public:
    virtual void Write(const Batch& batch) = 0;
    virtual void Flush() = 0;
};
