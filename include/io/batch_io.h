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
    virtual ~BatchReader() = default;
    virtual std::optional<Batch> ReadNext() = 0;
};

class BatchWriter {
   public:
    virtual ~BatchWriter() = default;
    virtual void Write(const Batch& batch) = 0;
    virtual void Flush() = 0;
};
