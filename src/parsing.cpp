#include "parsing.h"

#include <charconv>

#include "error.h"
#include "utils.h"

int64_t ParseInt64(const std::string& value) {
    if (value.empty()) {
        throw error::MakeError("columnar", "empty value for int64");
    }

    int64_t result = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();

    if (const auto [ptr, ec] = std::from_chars(begin, end, result); ec != std::errc() || ptr != end) {
        throw error::MakeError("columnar", "invalid int64 value");
    }

    return result;
}

ColumnType ParseColumnType(const std::string_view input) {
    const std::string lowered = ToLower(input);
    if (lowered == "int64") {
        return ColumnType::Int64;
    }
    if (lowered == "string") {
        return ColumnType::String;
    }
    throw error::MakeError("columnar", "unknown column type");
}

std::string ColumnTypeToString(const ColumnType type) {
    switch (type) {
        case ColumnType::Int64:
            return "int64";
        case ColumnType::String:
            return "string";
    }
    return "string";
}
