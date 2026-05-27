#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/int128.h"

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

enum class ExprKind {
    Column,
    Literal,
    Binary,
    Function,
    Star,
    Case,
};

enum class BinaryOp {
    Add,
    Subtract,
};

struct ExprSpec;
using ExprPtr = std::shared_ptr<ExprSpec>;

struct PredicateSpec;
using PredicatePtr = std::shared_ptr<PredicateSpec>;

struct CaseSpec {
    PredicatePtr condition;
    ExprPtr then_expr;
    ExprPtr else_expr;
};

struct ExprSpec {
    ExprKind kind = ExprKind::Literal;

    ColumnRef column;
    bool column_index_bound = false;
    size_t column_index = 0;

    QueryLiteral literal;

    BinaryOp binary_op = BinaryOp::Add;
    ExprPtr left;
    ExprPtr right;

    std::string function_name;
    std::vector<ExprPtr> arguments;

    bool regex_replace_bound = false;
    std::regex regex_pattern;
    std::string regex_replacement;

    CaseSpec case_spec;

    std::string output_name;
};

enum class PredicateKind {
    Comparison,
    And,
    Like,
    NotLike,
    In,
};

struct PredicateSpec {
    PredicateKind kind = PredicateKind::Comparison;

    ExprPtr left;
    ComparisonKind comparison = ComparisonKind::Equal;
    ExprPtr right;

    std::vector<ExprPtr> values;

    PredicatePtr lhs;
    PredicatePtr rhs;

    bool typed_literal_comparison_bound = false;
    size_t typed_column_index = 0;
    Int128 typed_literal_value = 0;
    ComparisonKind typed_comparison = ComparisonKind::Equal;

    bool literal_in_set_bound = false;
    size_t in_column_index = 0;
    std::unordered_set<std::string> literal_in_values;

    bool literal_like_pattern_bound = false;
    size_t like_column_index = 0;
    std::string like_pattern;
    bool like_negated = false;
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
    ExprPtr argument;
    std::string output_name;
};

enum class SelectItemKind {
    GroupKey,
    Aggregate,
};

struct SelectItemSpec {
    SelectItemKind kind = SelectItemKind::GroupKey;
    ColumnRef column;
    ExprPtr expression;
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
    std::vector<ExprPtr> group_by;

    PredicatePtr filter;
    PredicatePtr having;

    std::vector<OrderBySpec> order_by;

    std::optional<size_t> limit;
    size_t offset = 0;
};
