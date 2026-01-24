#include "utils.h"

#include <limits>

#include "schema.h"
#include "error.h"

std::string ToLower(const std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (const unsigned char ch : input) {
        output.push_back(static_cast<char>(std::tolower(ch)));
    }
    return output;
}

bool NeedsQuotes(const std::string_view value) { return value.find_first_of(",\"\n\r") != std::string_view::npos; }

bool SchemasEqual(const Schema& left, const Schema& right) {
    if (left.columns.size() != right.columns.size()) {
        return false;
    }

    for (size_t i = 0; i < left.columns.size(); ++i) {
        if (left.columns[i].name != right.columns[i].name) {
            return false;
        }
        if (left.columns[i].type != right.columns[i].type) {
            return false;
        }
    }

    return true;
}

uint64_t AddBytesChecked(const uint64_t current, const uint64_t add) {
    if (add > std::numeric_limits<uint64_t>::max() - current) {
        throw error::MakeError("batch", "batch size overflow");
    }
    return current + add;
}

uint64_t EstimateRowBytes(const Schema& schema, const std::vector<std::string>& row) {
    uint64_t bytes = 0;
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        switch (schema.columns[i].type) {
            case ColumnType::Int64:
                bytes += sizeof(int64_t);
                break;
            case ColumnType::String:
                bytes += static_cast<uint64_t>(row[i].size());
                break;
        }
    }
    return bytes;
}

void CheckIndex(const std::string& module, const size_t row, const size_t size) {
    if (row >= size) {
        throw error::MakeError(module, "row index out of range");
    }
}
