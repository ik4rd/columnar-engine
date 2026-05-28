#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/int128.h"
#include "executor/aggregate_function.h"
#include "executor/query.h"
#include "model/schema.h"

struct PlannedAgg {
    const AggFuncDefinition* function = nullptr;
    bool distinct = false;

    AggArgumentKind argument_kind = AggArgumentKind::Column;
    size_t column_index = 0;

    bool direct_numeric_argument = false;
    Int128 direct_numeric_offset = 0;

    ColumnType input_type = ColumnType::String;
    ExprPtr argument;

    std::string output_name;
};

struct PlannedGroupKey {
    size_t column_index = 0;
    ExprPtr expression;

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
    PlannedValue right;

    ComparisonKind comparison = ComparisonKind::Equal;
    ColumnType comparison_type = ColumnType::String;
};

struct PlannedPredicate {
    PredicatePtr predicate;
};

struct PlannedQuery {
    std::filesystem::path table_path;
    Schema table_schema;

    std::vector<size_t> projection_indexes;

    PredicatePtr filter;
    PredicatePtr having;

    std::vector<PlannedGroupKey> group_keys;
    std::vector<PlannedAgg> aggregates;
    std::vector<PlannedSelectItem> select_items;

    std::vector<SelectItemSpec> plain_select_items;

    std::vector<PlannedOrderBy> order_by;
    std::vector<OrderBySpec> plain_order_by;

    std::optional<size_t> limit;
    size_t offset = 0;

    bool plain_select = false;
    bool metadata_count_only = false;
    bool metadata_extrema_only = false;
};
