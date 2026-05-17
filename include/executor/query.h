#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

enum class ComparisonKind {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
};

enum class LiteralKind {
    Numeric,
    String,
    Identifier,
};

struct QueryLiteral {
    std::string text;
    LiteralKind kind = LiteralKind::String;
};

struct ColumnRef {
    std::string qualifier;
    std::string name;
};

enum class QueryValueKind {
    Column,
    Literal,
};

struct QueryValue {
    QueryValueKind kind = QueryValueKind::Literal;
    ColumnRef column;
    QueryLiteral literal;
};

struct FilterSpec {
    QueryValue left;
    ComparisonKind comparison = ComparisonKind::Equal;
    QueryValue right;
};

enum class AggArgumentKind {
    Column,
    Star,
};

struct AggSpec {
    std::string function_name;
    bool distinct = false;
    AggArgumentKind argument_kind = AggArgumentKind::Column;
    ColumnRef column;
    std::string output_name;
};

enum class SelectItemKind {
    GroupKey,
    Aggregate,
};

struct SelectItemSpec {
    SelectItemKind kind = SelectItemKind::GroupKey;
    ColumnRef column;
    AggSpec aggregate;
    std::string output_name;
};

struct OrderBySpec {
    SelectItemSpec item;
    bool descending = false;
};

struct Query {
    std::string table_name;
    std::string table_alias;
    std::vector<SelectItemSpec> select_items;
    std::vector<ColumnRef> group_by;
    std::optional<FilterSpec> filter;
    std::vector<OrderBySpec> order_by;
    std::optional<size_t> limit;
};
