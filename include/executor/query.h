#pragma once

#include <cstddef>
#include <memory>
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
    QueryLiteral literal;
    BinaryOp binary_op = BinaryOp::Add;
    ExprPtr left;
    ExprPtr right;
    std::string function_name;
    std::vector<ExprPtr> arguments;
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
