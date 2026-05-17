#pragma once

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "executor/aggregate_function.h"
#include "executor/query.h"
#include "model/schema.h"

struct PlannedAgg {
    const AggFuncDefinition* function = nullptr;
    bool distinct = false;
    AggArgumentKind argument_kind = AggArgumentKind::Column;
    size_t column_index = 0;
    ColumnType input_type = ColumnType::String;
    std::string output_name;
};

struct PlannedGroupKey {
    size_t column_index = 0;
    ColumnType column_type = ColumnType::String;
    std::string output_name;
};

struct PlannedSelectItem {
    SelectItemKind kind = SelectItemKind::GroupKey;
    size_t index = 0;
    ColumnType output_type = ColumnType::String;
    std::string output_name;
};

struct PlannedOrderBy {
    size_t result_column_index = 0;
    bool descending = false;
    ColumnType value_type = ColumnType::String;
};

enum class PlannedValueKind {
    Column,
    Literal,
};

struct PlannedValue {
    PlannedValueKind kind = PlannedValueKind::Literal;
    ColumnType column_type = ColumnType::String;
    size_t column_index = 0;
    std::string literal_text;
};

struct PlannedFilter {
    PlannedValue left;
    ComparisonKind comparison = ComparisonKind::Equal;
    PlannedValue right;
    ColumnType comparison_type = ColumnType::String;
};

struct PlannedQuery {
    std::filesystem::path table_path;
    std::vector<size_t> projection_indexes;
    std::optional<PlannedFilter> filter;
    std::vector<PlannedGroupKey> group_keys;
    std::vector<PlannedAgg> aggregates;
    std::vector<PlannedSelectItem> select_items;
    std::vector<PlannedOrderBy> order_by;
    std::optional<size_t> limit;
};
