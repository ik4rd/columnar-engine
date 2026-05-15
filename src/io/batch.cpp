#include <string>
#include <string_view>

#include "io/batch.h"
#include "support/error.h"

static void ValidateOptionalLimit(const std::optional<size_t>& value, const std::string_view name) {
    if (value && *value == 0) {
        throw Error::InvalidArgument("batch_io", std::string(name) + " must be > 0");
    }
}

bool BatchSizing::WouldExceed(const size_t next_rows, const size_t column_count, const uint64_t next_bytes) const {
    ValidateOptionalLimit(max_rows, "max rows");
    ValidateOptionalLimit(max_values, "max values");
    if (max_bytes && *max_bytes == 0) {
        throw Error::InvalidArgument("batch_io", "max bytes must be > 0");
    }

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
