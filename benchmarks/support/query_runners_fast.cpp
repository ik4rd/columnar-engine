#include <algorithm>
#include <compare>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/parsing.h"
#include "common/string_arena.h"
#include "executor/operators_internal.h"
#include "executor/query_utils.h"
#include "io/columnar_batch.h"
#include "query_runners.h"

namespace detail {

constexpr uint64_t hash_magic = 0x9e3779b97f4a7c15ULL;

struct Int128Hash {
    size_t operator()(const Int128 value) const noexcept {
        const UInt128 raw = static_cast<UInt128>(value);
        const uint64_t low = static_cast<uint64_t>(raw);
        const uint64_t high = static_cast<uint64_t>(raw >> 64);
        return std::hash<uint64_t>{}(low ^ (high + hash_magic + (low << 6) + (low >> 2)));
    }
};

struct Int128PairHash {
    size_t operator()(const std::pair<Int128, Int128>& value) const noexcept {
        const size_t lhs = Int128Hash{}(value.first);
        const size_t rhs = Int128Hash{}(value.second);
        return lhs ^ (rhs + hash_magic + (lhs << 6) + (lhs >> 2));
    }
};

struct SearchPhraseGroup {
    std::string_view key;
    std::string_view min_url;

    uint64_t count = 0;
    size_t ordinal = 0;
};

struct SearchPhraseStats {
    std::string_view key;
    std::string_view min_url;
    std::string_view min_title;

    std::unordered_set<Int128, Int128Hash> distinct_users;

    uint64_t count = 0;
    size_t ordinal = 0;
};

struct IntGroupStats {
    Int128 key1 = 0;
    Int128 key2 = 0;

    uint64_t count = 0;
    Int128 sum_refresh = 0;
    Int128 sum_width = 0;

    size_t ordinal = 0;
};

struct SearchPhraseOrderRow {
    Int128 event_time = 0;
    std::string_view search_phrase;

    size_t ordinal = 0;
};

}  // namespace detail

using detail::Int128Hash;
using detail::Int128PairHash;
using detail::IntGroupStats;
using detail::SearchPhraseGroup;
using detail::SearchPhraseOrderRow;
using detail::SearchPhraseStats;

static Batch SingleRow(const Schema& schema, const std::initializer_list<std::string_view> values) {
    Batch batch(schema, 1);
    size_t column = 0;

    for (const std::string_view value : values) {
        batch.AppendValueFromString(column++, value);
    }

    return batch;
}

static std::unique_ptr<Operator> ScanColumns(const std::filesystem::path& path, const Schema& schema,
                                             const std::initializer_list<std::string_view> columns) {
    std::vector<size_t> projection_indexes;
    projection_indexes.reserve(columns.size());

    for (const std::string_view column : columns) {
        projection_indexes.push_back(FindColumnIndex(schema, column));
    }

    return CreateScanOperator(path, std::move(projection_indexes), {});
}

static bool Contains(const std::string_view haystack, const std::string_view needle) {
    return haystack.find(needle) != std::string_view::npos;
}

template <typename Map>
static std::vector<const typename Map::mapped_type*> OrderGroupsByCount(const Map& groups, const size_t limit) {
    std::vector<const typename Map::mapped_type*> ordered;
    ordered.reserve(groups.size());

    for (const auto& entry : groups) {
        ordered.push_back(&entry.second);
    }

    std::ranges::sort(ordered, [](const auto* lhs, const auto* rhs) {
        if (lhs->count != rhs->count) {
            return lhs->count > rhs->count;
        }
        return lhs->ordinal < rhs->ordinal;
    });

    if (ordered.size() > limit) {
        ordered.resize(limit);
    }

    return ordered;
}

static ExecuteExpected CountAll(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    uint64_t rows = 0;

    for (const auto& row_group : reader.GetMetadata().row_groups) {
        rows += row_group.row_count;
    }

    return SingleRow(Schema{{ColumnSchema("COUNT(*)", ColumnType::Int64)}}, {std::to_string(rows)});
}

static ExecuteExpected CountAdvEngineNonZero(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"AdvEngineID"});

    uint64_t count = 0;

    while (const auto batch = scan->Next()) {
        const Column& adv_engine = batch->ColumnAt(0);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (adv_engine.ValueAsInt128(row) != 0) {
                ++count;
            }
        }
    }

    return SingleRow(Schema{{ColumnSchema("COUNT(*)", ColumnType::Int64)}}, {std::to_string(count)});
}

static ExecuteExpected SumCountAvg(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"AdvEngineID", "ResolutionWidth"});

    Int128 adv_sum = 0;
    Int128 width_sum = 0;
    uint64_t count = 0;

    while (const auto batch = scan->Next()) {
        const Column& adv_engine = batch->ColumnAt(0);
        const Column& width = batch->ColumnAt(1);
        count += batch->RowsCount();
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            adv_sum += adv_engine.ValueAsInt128(row);
            width_sum += width.ValueAsInt128(row);
        }
    }

    return SingleRow(
        Schema{{ColumnSchema("SUM(AdvEngineID)", ColumnType::Int128), ColumnSchema("COUNT(*)", ColumnType::Int64),
                ColumnSchema("AVG(ResolutionWidth)", ColumnType::Int128)}},
        {Int128ToString(adv_sum), std::to_string(count),
         count == 0 ? "0" : Int128ToString(width_sum / static_cast<Int128>(count))});
}

static ExecuteExpected AvgUserId(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"UserID"});

    Int128 sum = 0;
    uint64_t count = 0;

    while (const auto batch = scan->Next()) {
        const Column& user_id = batch->ColumnAt(0);
        count += batch->RowsCount();
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            sum += user_id.ValueAsInt128(row);
        }
    }

    return SingleRow(Schema{{ColumnSchema("AVG(UserID)", ColumnType::Int128)}},
                     {count == 0 ? "0" : Int128ToString(sum / static_cast<Int128>(count))});
}

static ExecuteExpected CountDistinctColumn(const std::filesystem::path& path, const std::string_view column_name,
                                           const std::string_view output_name) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {column_name});

    std::unordered_set<std::string> values;

    while (const auto batch = scan->Next()) {
        const Column& column = batch->ColumnAt(0);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            values.insert(column.ValueAsString(row));
        }
    }

    return SingleRow(Schema{{ColumnSchema(std::string(output_name), ColumnType::Int64)}},
                     {std::to_string(values.size())});
}

static ExecuteExpected MinMaxEventDate(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const size_t event_date = FindColumnIndex(reader.GetSchema(), "EventDate");

    bool has_value = false;
    Int128 min_value = 0;
    Int128 max_value = 0;

    for (const auto& row_group : reader.GetMetadata().row_groups) {
        const auto& chunk = row_group.columns[event_date];
        if (!chunk.has_min_max) {
            continue;
        }
        if (!has_value) {
            min_value = chunk.min_value;
            max_value = chunk.max_value;
            has_value = true;
        } else {
            min_value = std::min(min_value, chunk.min_value);
            max_value = std::max(max_value, chunk.max_value);
        }
    }

    return SingleRow(
        Schema{{ColumnSchema("MIN(EventDate)", ColumnType::Date), ColumnSchema("MAX(EventDate)", ColumnType::Date)}},
        {DateToString(static_cast<int32_t>(min_value)), DateToString(static_cast<int32_t>(max_value))});
}

static ExecuteExpected CountUrlContainsGoogle(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"URL"});

    uint64_t count = 0;

    while (const auto batch = scan->Next()) {
        const Column& url = batch->ColumnAt(0);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (Contains(url.ValueAsString(row), "google")) {
                ++count;
            }
        }
    }

    return SingleRow(Schema{{ColumnSchema("COUNT(*)", ColumnType::Int64)}}, {std::to_string(count)});
}

static ExecuteExpected SearchPhraseGroupByUrlContainsGoogle(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"URL", "SearchPhrase"});

    StringArena arena;
    std::unordered_map<std::string_view, SearchPhraseGroup, StringViewHash, StringViewEqual> groups;
    size_t ordinal = 0;

    while (const auto batch = scan->Next()) {
        const Column& url = batch->ColumnAt(0);
        const Column& search_phrase = batch->ColumnAt(1);

        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (!Contains(url.ValueAsString(row), "google")) {
                continue;
            }

            const std::string_view key = arena.Store(search_phrase.ValueAsString(row));
            if (key.empty()) {
                continue;
            }

            auto& group = groups[key];
            if (group.count == 0) {
                group.key = key;
                group.ordinal = ordinal++;
                group.min_url = arena.Store(url.ValueAsString(row));
            } else if (url.ValueAsString(row) < group.min_url) {
                group.min_url = arena.Store(url.ValueAsString(row));
            }

            ++group.count;
        }
    }

    const auto ordered = OrderGroupsByCount(groups, 10);
    Batch result(Schema{{ColumnSchema("SearchPhrase", ColumnType::String), ColumnSchema("MIN(URL)", ColumnType::String),
                         ColumnSchema("c", ColumnType::Int64)}},
                 ordered.size());

    for (const SearchPhraseGroup* group : ordered) {
        result.AppendValueFromString(0, group->key);
        result.AppendValueFromString(1, group->min_url);
        result.AppendValueFromString(2, std::to_string(group->count));
    }

    return result;
}

static ExecuteExpected SearchPhraseGroupByTitleGoogle(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"Title", "URL", "SearchPhrase", "UserID"});

    StringArena arena;
    std::unordered_map<std::string_view, SearchPhraseStats, StringViewHash, StringViewEqual> groups;
    size_t ordinal = 0;

    while (const auto batch = scan->Next()) {
        const Column& title = batch->ColumnAt(0);
        const Column& url = batch->ColumnAt(1);
        const Column& search_phrase = batch->ColumnAt(2);
        const Column& user_id = batch->ColumnAt(3);

        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            const std::string_view title_value = arena.Store(title.ValueAsString(row));
            if (!Contains(title_value, "Google")) {
                continue;
            }

            const std::string_view url_value = arena.Store(url.ValueAsString(row));
            if (Contains(url_value, ".google.")) {
                continue;
            }

            const std::string_view key = arena.Store(search_phrase.ValueAsString(row));
            if (key.empty()) {
                continue;
            }

            auto& group = groups[key];
            if (group.count == 0) {
                group.key = key;
                group.ordinal = ordinal++;
                group.min_url = url_value;
                group.min_title = title_value;
            } else {
                if (url_value < group.min_url) {
                    group.min_url = url_value;
                }
                if (title_value < group.min_title) {
                    group.min_title = title_value;
                }
            }

            group.distinct_users.insert(user_id.ValueAsInt128(row));
            ++group.count;
        }
    }

    const auto ordered = OrderGroupsByCount(groups, 10);
    Batch result(Schema{{ColumnSchema("SearchPhrase", ColumnType::String), ColumnSchema("MIN(URL)", ColumnType::String),
                         ColumnSchema("MIN(Title)", ColumnType::String), ColumnSchema("c", ColumnType::Int64),
                         ColumnSchema("COUNT(DISTINCT UserID)", ColumnType::Int64)}},
                 ordered.size());

    for (const SearchPhraseStats* group : ordered) {
        result.AppendValueFromString(0, group->key);
        result.AppendValueFromString(1, group->min_url);
        result.AppendValueFromString(2, group->min_title);
        result.AppendValueFromString(3, std::to_string(group->count));
        result.AppendValueFromString(4, std::to_string(group->distinct_users.size()));
    }

    return result;
}

static ExecuteExpected SumWidthVariants(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"ResolutionWidth"});

    Int128 width_sum = 0;
    uint64_t row_count = 0;

    while (const auto batch = scan->Next()) {
        const Column& width = batch->ColumnAt(0);
        row_count += batch->RowsCount();
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            width_sum += width.ValueAsInt128(row);
        }
    }

    std::vector<ColumnSchema> schema_columns;
    std::vector<std::string> values;

    schema_columns.emplace_back("SUM(ResolutionWidth)", ColumnType::Int128);
    values.push_back(Int128ToString(width_sum));

    for (int offset = 1; offset <= 89; ++offset) {
        schema_columns.emplace_back("SUM(ResolutionWidth + " + std::to_string(offset) + ")", ColumnType::Int128);
        values.push_back(Int128ToString(width_sum + static_cast<Int128>(offset) * static_cast<Int128>(row_count)));
    }

    Batch result(Schema{schema_columns}, 1);
    for (size_t i = 0; i < values.size(); ++i) {
        result.AppendValueFromString(i, values[i]);
    }

    return result;
}

static ExecuteExpected GroupByIntPairWithMetrics(const std::filesystem::path& path, const std::string_view key1_name,
                                                 const std::string_view key2_name, const bool filter_search_phrase) {
    const ColumnarBatchReader reader(path);
    const auto scan =
        ScanColumns(path, reader.GetSchema(), {key1_name, key2_name, "IsRefresh", "ResolutionWidth", "SearchPhrase"});

    std::unordered_map<std::pair<Int128, Int128>, IntGroupStats, Int128PairHash> groups;
    size_t ordinal = 0;

    while (const auto batch = scan->Next()) {
        const Column& key1 = batch->ColumnAt(0);
        const Column& key2 = batch->ColumnAt(1);
        const Column& is_refresh = batch->ColumnAt(2);
        const Column& width = batch->ColumnAt(3);
        const Column& search_phrase = batch->ColumnAt(4);

        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (filter_search_phrase && search_phrase.ValueAsString(row).empty()) {
                continue;
            }

            const std::pair<Int128, Int128> key{key1.ValueAsInt128(row), key2.ValueAsInt128(row)};

            auto& group = groups[key];
            if (group.count == 0) {
                group.key1 = key.first;
                group.key2 = key.second;
                group.ordinal = ordinal++;
            }

            ++group.count;
            group.sum_refresh += is_refresh.ValueAsInt128(row);
            group.sum_width += width.ValueAsInt128(row);
        }
    }

    const auto ordered = OrderGroupsByCount(groups, 10);
    Batch result(Schema{{ColumnSchema(std::string(key1_name), ColumnType::Int128),
                         ColumnSchema(std::string(key2_name), ColumnType::Int128), ColumnSchema("c", ColumnType::Int64),
                         ColumnSchema("SUM(IsRefresh)", ColumnType::Int128),
                         ColumnSchema("AVG(ResolutionWidth)", ColumnType::Int128)}},
                 ordered.size());

    for (const IntGroupStats* group : ordered) {
        result.AppendValueFromString(0, Int128ToString(group->key1));
        result.AppendValueFromString(1, Int128ToString(group->key2));
        result.AppendValueFromString(2, std::to_string(group->count));
        result.AppendValueFromString(3, Int128ToString(group->sum_refresh));
        result.AppendValueFromString(
            4, group->count == 0 ? "0" : Int128ToString(group->sum_width / static_cast<Int128>(group->count)));
    }

    return result;
}

static ExecuteExpected GroupByURLCount(const std::filesystem::path& path, const bool add_constant_column) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"URL"});

    StringArena arena;
    std::unordered_map<std::string_view, SearchPhraseStats, StringViewHash, StringViewEqual> groups;
    size_t ordinal = 0;

    while (const auto batch = scan->Next()) {
        const Column& url = batch->ColumnAt(0);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            const std::string_view key = arena.Store(url.ValueAsString(row));
            auto& group = groups[key];
            if (group.count == 0) {
                group.key = key;
                group.ordinal = ordinal++;
            }
            ++group.count;
        }
    }

    const auto ordered = OrderGroupsByCount(groups, 10);
    Batch result(add_constant_column
                     ? Schema{{ColumnSchema("1", ColumnType::Int64), ColumnSchema("URL", ColumnType::String),
                               ColumnSchema("COUNT(*)", ColumnType::Int64)}}
                     : Schema{{ColumnSchema("URL", ColumnType::String), ColumnSchema("COUNT(*)", ColumnType::Int64)}},
                 ordered.size());

    for (const SearchPhraseStats* group : ordered) {
        if (add_constant_column) {
            result.AppendValueFromString(0, "1");
            result.AppendValueFromString(1, group->key);
            result.AppendValueFromString(2, std::to_string(group->count));
        } else {
            result.AppendValueFromString(0, group->key);
            result.AppendValueFromString(1, std::to_string(group->count));
        }
    }

    return result;
}

static ExecuteExpected GroupByClientIPDerived(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"ClientIP"});

    std::unordered_map<Int128, IntGroupStats, Int128Hash> groups;
    size_t ordinal = 0;

    while (const auto batch = scan->Next()) {
        const Column& client_ip = batch->ColumnAt(0);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            const Int128 key = client_ip.ValueAsInt128(row);
            auto& group = groups[key];
            if (group.count == 0) {
                group.key1 = key;
                group.ordinal = ordinal++;
            }
            ++group.count;
        }
    }

    const auto ordered = OrderGroupsByCount(groups, 10);
    Batch result(
        Schema{{ColumnSchema("ClientIP", ColumnType::Int128), ColumnSchema("ClientIP - 1", ColumnType::Int128),
                ColumnSchema("ClientIP - 2", ColumnType::Int128), ColumnSchema("ClientIP - 3", ColumnType::Int128),
                ColumnSchema("COUNT(*)", ColumnType::Int64)}},
        ordered.size());

    for (const IntGroupStats* group : ordered) {
        const Int128 key = group->key1;
        result.AppendValueFromString(0, Int128ToString(key));
        result.AppendValueFromString(1, Int128ToString(key - 1));
        result.AppendValueFromString(2, Int128ToString(key - 2));
        result.AppendValueFromString(3, Int128ToString(key - 3));
        result.AppendValueFromString(4, std::to_string(group->count));
    }

    return result;
}

static std::vector<SearchPhraseOrderRow> CollectSearchPhraseOrderRows(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"EventTime", "SearchPhrase"});

    StringArena arena;
    std::vector<SearchPhraseOrderRow> rows;
    size_t ordinal = 0;

    while (const auto batch = scan->Next()) {
        const Column& event_time = batch->ColumnAt(0);
        const Column& search_phrase = batch->ColumnAt(1);

        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            const std::string_view phrase = arena.Store(search_phrase.ValueAsString(row));
            if (phrase.empty()) {
                continue;
            }
            rows.push_back(SearchPhraseOrderRow{
                .event_time = event_time.ValueAsInt128(row), .search_phrase = phrase, .ordinal = ordinal++});
        }
    }

    return rows;
}

static ExecuteExpected SelectSearchPhraseOrderByEventTime(const std::filesystem::path& path) {
    auto rows = CollectSearchPhraseOrderRows(path);

    std::ranges::sort(rows, [](const SearchPhraseOrderRow& lhs, const SearchPhraseOrderRow& rhs) {
        if (lhs.event_time != rhs.event_time) {
            return lhs.event_time < rhs.event_time;
        }
        return lhs.ordinal < rhs.ordinal;
    });

    if (rows.size() > 10) {
        rows.resize(10);
    }

    Batch result(
        Schema{{ColumnSchema("SearchPhrase", ColumnType::String), ColumnSchema("EventTime", ColumnType::Timestamp)}},
        rows.size());

    for (const auto& row : rows) {
        result.AppendValueFromString(0, row.search_phrase);
        result.AppendValueFromString(1, TimestampToString(static_cast<int64_t>(row.event_time)));
    }

    return result;
}

static ExecuteExpected SelectSearchPhraseOrderByPhrase(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"SearchPhrase"});

    StringArena arena;
    std::vector<std::string_view> rows;

    while (const auto batch = scan->Next()) {
        const Column& search_phrase = batch->ColumnAt(0);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            const std::string_view phrase = arena.Store(search_phrase.ValueAsString(row));
            if (phrase.empty()) {
                continue;
            }
            rows.push_back(phrase);
        }
    }

    std::ranges::sort(rows);

    if (rows.size() > 10) {
        rows.resize(10);
    }

    Batch result(Schema{{ColumnSchema("SearchPhrase", ColumnType::String)}}, rows.size());
    for (const std::string_view row : rows) {
        result.AppendValueFromString(0, row);
    }

    return result;
}

static ExecuteExpected SelectSearchPhraseOrderByEventTimeThenPhrase(const std::filesystem::path& path) {
    auto rows = CollectSearchPhraseOrderRows(path);

    std::ranges::sort(rows, [](const SearchPhraseOrderRow& lhs, const SearchPhraseOrderRow& rhs) {
        if (lhs.event_time != rhs.event_time) {
            return lhs.event_time < rhs.event_time;
        }
        if (lhs.search_phrase != rhs.search_phrase) {
            return lhs.search_phrase < rhs.search_phrase;
        }
        return lhs.ordinal < rhs.ordinal;
    });

    if (rows.size() > 10) {
        rows.resize(10);
    }

    Batch result(
        Schema{{ColumnSchema("SearchPhrase", ColumnType::String), ColumnSchema("EventTime", ColumnType::Timestamp)}},
        rows.size());

    for (const auto& row : rows) {
        result.AppendValueFromString(0, row.search_phrase);
        result.AppendValueFromString(1, TimestampToString(static_cast<int64_t>(row.event_time)));
    }

    return result;
}

namespace clickbench_fast {

std::optional<ExecuteExpected> TryRun(const std::filesystem::path& path, const int query_id) {
    switch (query_id) {
        case 0:
            return CountAll(path);
        case 1:
            return CountAdvEngineNonZero(path);
        case 2:
            return SumCountAvg(path);
        case 3:
            return AvgUserId(path);
        case 4:
            return CountDistinctColumn(path, "UserID", "COUNT(DISTINCT UserID)");
        case 5:
            return CountDistinctColumn(path, "SearchPhrase", "COUNT(DISTINCT SearchPhrase)");
        case 6:
            return MinMaxEventDate(path);
        case 20:
            return CountUrlContainsGoogle(path);
        case 21:
            return SearchPhraseGroupByUrlContainsGoogle(path);
        case 22:
            return SearchPhraseGroupByTitleGoogle(path);
        case 24:
            return SelectSearchPhraseOrderByEventTime(path);
        case 25:
            return SelectSearchPhraseOrderByPhrase(path);
        case 26:
            return SelectSearchPhraseOrderByEventTimeThenPhrase(path);
        case 29:
            return SumWidthVariants(path);
        case 30:
            return GroupByIntPairWithMetrics(path, "SearchEngineID", "ClientIP", true);
        case 31:
            return GroupByIntPairWithMetrics(path, "WatchID", "ClientIP", true);
        case 32:
            return GroupByIntPairWithMetrics(path, "WatchID", "ClientIP", false);
        case 33:
            return GroupByURLCount(path, false);
        case 34:
            return GroupByURLCount(path, true);
        case 35:
            return GroupByClientIPDerived(path);
        default:
            return std::nullopt;
    }
}

}  // namespace clickbench_fast
