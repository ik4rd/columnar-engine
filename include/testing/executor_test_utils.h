#pragma once

#include <string>
#include <vector>

#include "io/csv_batch.h"

inline std::vector<std::string> SingleRowValues(const Batch& batch) {
    std::vector<std::vector<std::string>> rows;
    rows.reserve(1);

    AppendBatchRows(batch, rows);

    if (rows.empty()) {
        return {};
    }

    return std::move(rows.front());
}

inline std::vector<std::vector<std::string>> BatchRows(const Batch& batch) {
    std::vector<std::vector<std::string>> rows;
    rows.reserve(batch.RowsCount());

    AppendBatchRows(batch, rows);

    return rows;
}

inline std::vector<std::string> BatchColumnNames(const Batch& batch) {
    std::vector<std::string> names;
    names.reserve(batch.ColumnsCount());

    for (const auto& column : batch.GetSchema().columns) {
        names.push_back(column.name);
    }

    return names;
}
