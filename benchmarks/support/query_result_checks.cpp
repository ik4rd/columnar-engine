#include "query_result_checks.h"

#include <algorithm>
#include <string>
#include <vector>

#include "io/csv_batch_io.h"
#include "support/ascii.h"

std::string BatchToString(const Batch& batch) {
    std::vector<std::vector<std::string>> rows;
    AppendBatchRows(batch, rows);

    std::string result;

    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            result += row[i];
            if (i + 1 < row.size()) {
                result += ",";
            }
        }
        result += "\n";
    }

    return result;
}

bool QueryUsesGroupBy(const std::string_view sql) { return ToUpperAscii(sql).find("GROUP BY") != std::string::npos; }

static std::vector<std::string> SplitRows(const std::string_view text) {
    std::vector<std::string> rows;
    size_t start = 0;

    while (start < text.size()) {
        const size_t end = text.find('\n', start);
        if (end == std::string_view::npos) {
            rows.emplace_back(text.substr(start));
            break;
        }
        rows.emplace_back(text.substr(start, end - start));
        start = end + 1;
    }

    if (!rows.empty() && rows.back().empty()) {
        rows.pop_back();
    }

    return rows;
}

bool EqualAsRowMultisets(const std::string_view lhs, const std::string_view rhs) {
    std::vector<std::string> lhs_rows = SplitRows(lhs);
    std::vector<std::string> rhs_rows = SplitRows(rhs);

    std::ranges::sort(lhs_rows);
    std::ranges::sort(rhs_rows);

    return lhs_rows == rhs_rows;
}
