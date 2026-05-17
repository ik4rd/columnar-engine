#include "query_result_checks.h"

#include <algorithm>
#include <cctype>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/ascii.h"
#include "common/error.h"
#include "executor/operator_utils.h"
#include "executor/planner_utils.h"
#include "io/csv.h"
#include "io/csv_batch.h"

std::string BatchToString(const Batch& batch) {
    std::vector<std::vector<std::string>> rows;
    AppendBatchRows(batch, rows);

    std::ostringstream out;
    const CsvWriter writer(out);

    for (const auto& row : rows) {
        writer.WriteRow(row);
    }

    writer.Flush();

    return out.str();
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

static std::string TrimCopy(const std::string_view value) {
    size_t begin = 0;
    while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin]))) {
        ++begin;
    }

    size_t end = value.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }

    return std::string(value.substr(begin, end - begin));
}

static std::vector<std::vector<std::string>> ParseCsvRows(const std::string_view text) {
    std::istringstream in{std::string(text)};
    const CsvReader reader(in);

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> row;

    while (reader.ReadRow(row)) {
        rows.push_back(row);
    }

    return rows;
}

struct ParsedOrderBy {
    std::string output_name;
    bool descending = false;
};

static std::optional<ParsedOrderBy> ParseSingleOrderBy(const std::string_view sql) {
    const std::string upper = ToUpperAscii(sql);
    const size_t order_pos = upper.find("ORDER BY");
    const size_t limit_pos = upper.find("LIMIT", order_pos == std::string::npos ? 0 : order_pos);

    if (order_pos == std::string::npos || limit_pos == std::string::npos || limit_pos <= order_pos) {
        return std::nullopt;
    }

    std::string item = TrimCopy(sql.substr(order_pos + std::string_view("ORDER BY").size(),
                                           limit_pos - order_pos - std::string_view("ORDER BY").size()));
    if (const size_t comma = item.find(','); comma != std::string::npos) {
        item = TrimCopy(std::string_view(item).substr(0, comma));
    }

    bool descending = false;
    const std::string upper_item = ToUpperAscii(item);
    if (upper_item.ends_with(" DESC")) {
        descending = true;
        item = TrimCopy(std::string_view(item).substr(0, item.size() - std::string_view(" DESC").size()));
    } else if (upper_item.ends_with(" ASC")) {
        item = TrimCopy(std::string_view(item).substr(0, item.size() - std::string_view(" ASC").size()));
    }

    if (item.empty()) {
        return std::nullopt;
    }

    return ParsedOrderBy{
        .output_name = std::move(item),
        .descending = descending,
    };
}

static std::optional<size_t> FindOutputColumn(const Schema& schema, const std::string_view name) {
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (SameColumnName(schema.columns[i].name, name)) {
            return i;
        }
    }
    return std::nullopt;
}

static std::string EncodeRowForCompare(const std::vector<std::string>& row) {
    std::string encoded;
    for (const auto& value : row) {
        encoded += std::to_string(value.size());
        encoded.push_back(':');
        encoded += value;
    }
    return encoded;
}

static bool EqualRowMultisets(const std::vector<std::vector<std::string>>& lhs,
                              const std::vector<std::vector<std::string>>& rhs) {
    std::vector<std::string> lhs_encoded;
    std::vector<std::string> rhs_encoded;
    lhs_encoded.reserve(lhs.size());
    rhs_encoded.reserve(rhs.size());

    for (const auto& row : lhs) {
        lhs_encoded.push_back(EncodeRowForCompare(row));
    }
    for (const auto& row : rhs) {
        rhs_encoded.push_back(EncodeRowForCompare(row));
    }

    std::ranges::sort(lhs_encoded);
    std::ranges::sort(rhs_encoded);

    return lhs_encoded == rhs_encoded;
}

static std::optional<std::string> RawFieldFromRight(const std::string& row, const size_t offset_from_right) {
    size_t end = row.size();

    for (size_t i = 0; i < offset_from_right; ++i) {
        const size_t comma = row.rfind(',', end == 0 ? 0 : end - 1);
        if (comma == std::string::npos) {
            return std::nullopt;
        }
        end = comma;
    }

    const size_t comma = row.rfind(',', end == 0 ? 0 : end - 1);
    const size_t begin = comma == std::string::npos ? 0 : comma + 1;
    return row.substr(begin, end - begin);
}

static std::string NormalizeRawBenchmarkRow(std::string row) {
    std::erase(row, '"');
    return row;
}

static bool EqualWithLimitTiesRawFallback(const Schema& schema, const ParsedOrderBy& order_by,
                                          const size_t order_column, const std::string_view actual,
                                          const std::string_view expected) {
    const std::vector<std::string> actual_rows = SplitRows(actual);
    const std::vector<std::string> expected_rows = SplitRows(expected);
    if (actual_rows.size() != expected_rows.size() || expected_rows.empty() || order_column >= schema.columns.size()) {
        return false;
    }

    const size_t offset_from_right = schema.columns.size() - 1 - order_column;
    const std::optional<std::string> cutoff = RawFieldFromRight(expected_rows.back(), offset_from_right);
    if (!cutoff.has_value()) {
        return false;
    }

    const ColumnType order_type = schema.columns[order_column].type;

    const auto is_above_cutoff = [&](const std::string& value) {
        const std::strong_ordering comparison = CompareValues(order_type, value, *cutoff);
        return order_by.descending ? comparison > 0 : comparison < 0;
    };

    const auto is_not_worse_than_cutoff = [&](const std::string& value) {
        const std::strong_ordering comparison = CompareValues(order_type, value, *cutoff);
        return order_by.descending ? comparison >= 0 : comparison <= 0;
    };

    std::vector<std::string> actual_above_cutoff;
    std::vector<std::string> expected_above_cutoff;

    for (const auto& row : expected_rows) {
        const std::optional<std::string> value = RawFieldFromRight(row, offset_from_right);
        if (!value.has_value()) {
            return false;
        }
        if (is_above_cutoff(*value)) {
            expected_above_cutoff.push_back(NormalizeRawBenchmarkRow(row));
        }
    }

    for (const auto& row : actual_rows) {
        const std::optional<std::string> value = RawFieldFromRight(row, offset_from_right);
        if (!value.has_value() || !is_not_worse_than_cutoff(*value)) {
            return false;
        }
        if (is_above_cutoff(*value)) {
            actual_above_cutoff.push_back(NormalizeRawBenchmarkRow(row));
        }
    }

    std::ranges::sort(actual_above_cutoff);
    std::ranges::sort(expected_above_cutoff);

    return actual_above_cutoff == expected_above_cutoff;
}

bool EqualWithLimitTies(const std::string_view sql, const Schema& schema, const std::string_view actual,
                        const std::string_view expected) {
    const std::optional<ParsedOrderBy> order_by = ParseSingleOrderBy(sql);
    if (!order_by.has_value()) {
        return false;
    }

    const std::optional<size_t> order_column = FindOutputColumn(schema, order_by->output_name);
    if (!order_column.has_value()) {
        return false;
    }

    std::vector<std::vector<std::string>> actual_rows;
    std::vector<std::vector<std::string>> expected_rows;

    try {
        actual_rows = ParseCsvRows(actual);
        expected_rows = ParseCsvRows(expected);
    } catch (const Error&) {
        return EqualWithLimitTiesRawFallback(schema, *order_by, *order_column, actual, expected);
    }

    try {
        if (actual_rows.size() != expected_rows.size() || expected_rows.empty()) {
            return false;
        }

        const ColumnType order_type = schema.columns[*order_column].type;
        const std::string& cutoff = expected_rows.back()[*order_column];

        std::vector<std::vector<std::string>> actual_above_cutoff;
        std::vector<std::vector<std::string>> expected_above_cutoff;

        const auto is_above_cutoff = [&](const std::string& value) {
            const std::strong_ordering comparison = CompareValues(order_type, value, cutoff);
            return order_by->descending ? comparison > 0 : comparison < 0;
        };

        const auto is_not_worse_than_cutoff = [&](const std::string& value) {
            const std::strong_ordering comparison = CompareValues(order_type, value, cutoff);
            return order_by->descending ? comparison >= 0 : comparison <= 0;
        };

        for (const auto& row : expected_rows) {
            if (is_above_cutoff(row[*order_column])) {
                expected_above_cutoff.push_back(row);
            }
        }

        for (const auto& row : actual_rows) {
            if (!is_not_worse_than_cutoff(row[*order_column])) {
                return false;
            }
            if (is_above_cutoff(row[*order_column])) {
                actual_above_cutoff.push_back(row);
            }
        }

        return EqualRowMultisets(actual_above_cutoff, expected_above_cutoff);
    } catch (const Error&) {
        return EqualWithLimitTiesRawFallback(schema, *order_by, *order_column, actual, expected);
    }
}
