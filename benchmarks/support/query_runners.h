#pragma once

#include <filesystem>
#include <initializer_list>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/parsing.h"
#include "common/string_arena.h"
#include "executor/executor.h"
#include "executor/operators_internal.h"
#include "executor/query.h"
#include "executor/query_utils.h"
#include "io/columnar_batch.h"

enum class QueryExecutionMode {
    Hardcoded,
    Parser,
};

namespace clickbench {

inline ExprPtr MakeExpr(const ExprKind kind, std::string output_name = {}) {
    auto expr = std::make_shared<ExprSpec>();
    expr->kind = kind;
    expr->output_name = std::move(output_name);
    return expr;
}

inline ExprPtr Col(std::string name) {
    auto expr = MakeExpr(ExprKind::Column, name);
    expr->column = ColumnRef{.qualifier = {}, .name = std::move(name)};
    return expr;
}

inline ExprPtr Num(std::string text) {
    auto expr = MakeExpr(ExprKind::Literal, text);
    expr->literal = QueryLiteral{.text = std::move(text), .kind = LiteralKind::Numeric};
    return expr;
}

inline ExprPtr Str(std::string text) {
    auto expr = MakeExpr(ExprKind::Literal, text);
    expr->literal = QueryLiteral{.text = std::move(text), .kind = LiteralKind::String};
    return expr;
}

inline ExprPtr Ident(std::string text) {
    auto expr = MakeExpr(ExprKind::Literal, text);
    expr->literal = QueryLiteral{.text = std::move(text), .kind = LiteralKind::Identifier};
    return expr;
}

inline ExprPtr Star() { return MakeExpr(ExprKind::Star, "*"); }

inline std::string FormatFunctionName(const std::string_view function, const std::vector<ExprPtr>& args) {
    std::string out(function);
    out += "(";
    for (size_t i = 0; i < args.size(); ++i) {
        if (i > 0) {
            out += ", ";
        }
        out += args[i]->output_name;
    }
    out += ")";
    return out;
}

inline ExprPtr Func(std::string name, std::vector<ExprPtr> args, std::string output_name = {}) {
    auto expr = MakeExpr(ExprKind::Function);
    expr->function_name = std::move(name);
    expr->arguments = std::move(args);
    expr->output_name =
        output_name.empty() ? FormatFunctionName(expr->function_name, expr->arguments) : std::move(output_name);
    return expr;
}

inline ExprPtr ExtractMinute(ExprPtr expr) {
    return Func("EXTRACT", {Ident("MINUTE"), std::move(expr)}, "EXTRACT(MINUTE, EventTime)");
}

inline ExprPtr DateTruncMinute(ExprPtr expr) {
    return Func("DATE_TRUNC", {Str("'minute'"), std::move(expr)}, "DATE_TRUNC('minute', EventTime)");
}

inline ExprPtr Binary(ExprPtr left, const BinaryOp op, ExprPtr right) {
    auto expr = MakeExpr(ExprKind::Binary);
    expr->binary_op = op;
    expr->output_name = left->output_name + (op == BinaryOp::Add ? " + " : " - ") + right->output_name;
    expr->left = std::move(left);
    expr->right = std::move(right);
    return expr;
}

inline ExprPtr Add(ExprPtr left, ExprPtr right) { return Binary(std::move(left), BinaryOp::Add, std::move(right)); }

inline ExprPtr Sub(ExprPtr left, ExprPtr right) {
    return Binary(std::move(left), BinaryOp::Subtract, std::move(right));
}

inline ExprPtr Case(PredicatePtr condition, ExprPtr then_expr, ExprPtr else_expr) {
    auto expr = MakeExpr(ExprKind::Case, "CASE");
    expr->case_spec = CaseSpec{
        .condition = std::move(condition),
        .then_expr = std::move(then_expr),
        .else_expr = std::move(else_expr),
    };
    return expr;
}

inline PredicatePtr Pred(const PredicateKind kind) {
    auto pred = std::make_shared<PredicateSpec>();
    pred->kind = kind;
    return pred;
}

inline PredicatePtr Cmp(ExprPtr left, const ComparisonKind comparison, ExprPtr right) {
    auto pred = Pred(PredicateKind::Comparison);
    pred->left = std::move(left);
    pred->comparison = comparison;
    pred->right = std::move(right);
    return pred;
}

inline PredicatePtr Like(ExprPtr left, ExprPtr pattern, const bool negated = false) {
    auto pred = Pred(negated ? PredicateKind::NotLike : PredicateKind::Like);
    pred->left = std::move(left);
    pred->right = std::move(pattern);
    return pred;
}

inline PredicatePtr In(ExprPtr left, std::vector<ExprPtr> values) {
    auto pred = Pred(PredicateKind::In);
    pred->left = std::move(left);
    pred->values = std::move(values);
    return pred;
}

inline PredicatePtr And(PredicatePtr lhs, PredicatePtr rhs) {
    auto pred = Pred(PredicateKind::And);
    pred->lhs = std::move(lhs);
    pred->rhs = std::move(rhs);
    return pred;
}

inline PredicatePtr All(const std::initializer_list<PredicatePtr> predicates) {
    auto it = predicates.begin();
    PredicatePtr result = *it++;
    for (; it != predicates.end(); ++it) {
        result = And(std::move(result), *it);
    }
    return result;
}

inline SelectItemSpec Select(ExprPtr expr, std::string alias = {}) {
    SelectItemSpec item;
    item.kind = SelectItemKind::GroupKey;
    item.column = expr->kind == ExprKind::Column ? expr->column : ColumnRef{};
    item.expression = std::move(expr);
    item.output_name = alias.empty() ? item.expression->output_name : std::move(alias);
    return item;
}

inline SelectItemSpec Agg(std::string function, ExprPtr argument, const bool distinct = false, std::string alias = {}) {
    AggSpec aggregate;
    aggregate.function_name = std::move(function);
    aggregate.distinct = distinct;
    aggregate.argument_kind = argument->kind == ExprKind::Star ? AggArgumentKind::Star : AggArgumentKind::Column;

    const std::string argument_name = argument->output_name;
    if (aggregate.argument_kind == AggArgumentKind::Column) {
        aggregate.argument = std::move(argument);
        if (aggregate.argument->kind == ExprKind::Column) {
            aggregate.column = aggregate.argument->column;
        }
    }

    aggregate.output_name = aggregate.function_name + "(";
    if (aggregate.argument_kind == AggArgumentKind::Star) {
        aggregate.output_name += "*";
    } else {
        if (aggregate.distinct) {
            aggregate.output_name += "DISTINCT ";
        }
        aggregate.output_name += argument_name;
    }
    aggregate.output_name += ")";

    SelectItemSpec item;
    item.kind = SelectItemKind::Aggregate;
    item.aggregate = std::move(aggregate);
    item.output_name = alias.empty() ? item.aggregate.output_name : std::move(alias);

    return item;
}

inline OrderBySpec Order(SelectItemSpec item, const bool descending = false) {
    return OrderBySpec{.item = std::move(item), .descending = descending};
}

inline OrderBySpec OrderExpr(ExprPtr expr, const bool descending = false) {
    return Order(Select(std::move(expr)), descending);
}

inline OrderBySpec OrderAgg(std::string function, ExprPtr argument, const bool descending = false) {
    return Order(Agg(std::move(function), std::move(argument)), descending);
}

inline Query Q(std::vector<SelectItemSpec> select_items, std::vector<ExprPtr> group_by = {}, PredicatePtr filter = {},
               std::vector<OrderBySpec> order_by = {}, const std::optional<size_t> limit = {}, const size_t offset = 0,
               PredicatePtr having = {}) {
    Query query;
    query.table_name = "hits";
    query.select_items = std::move(select_items);
    query.group_by = std::move(group_by);
    query.filter = std::move(filter);
    query.having = std::move(having);
    query.order_by = std::move(order_by);
    query.limit = limit;
    query.offset = offset;
    return query;
}

inline std::optional<Query> BuildClickBenchQuery(const int id) {
    switch (id) {
        case 0:
            return Q({Agg("COUNT", Star())});
        case 1:
            return Q({Agg("COUNT", Star())}, {}, Cmp(Col("AdvEngineID"), ComparisonKind::NotEqual, Num("0")));
        case 2:
            return Q({Agg("SUM", Col("AdvEngineID")), Agg("COUNT", Star()), Agg("AVG", Col("ResolutionWidth"))});
        case 3:
            return Q({Agg("AVG", Col("UserID"))});
        case 4:
            return Q({Agg("COUNT", Col("UserID"), true)});
        case 5:
            return Q({Agg("COUNT", Col("SearchPhrase"), true)});
        case 6:
            return Q({Agg("MIN", Col("EventDate")), Agg("MAX", Col("EventDate"))});
        case 7:
            return Q({Select(Col("AdvEngineID")), Agg("COUNT", Star())}, {Col("AdvEngineID")},
                     Cmp(Col("AdvEngineID"), ComparisonKind::NotEqual, Num("0")), {OrderAgg("COUNT", Star(), true)});
        case 8:
            return Q({Select(Col("RegionID")), Agg("COUNT", Col("UserID"), true, "u")}, {Col("RegionID")}, {},
                     {OrderExpr(Col("u"), true)}, 10);
        case 9:
            return Q({Select(Col("RegionID")), Agg("SUM", Col("AdvEngineID")), Agg("COUNT", Star(), false, "c"),
                      Agg("AVG", Col("ResolutionWidth")), Agg("COUNT", Col("UserID"), true)},
                     {Col("RegionID")}, {}, {OrderExpr(Col("c"), true)}, 10);
        case 10:
            return Q({Select(Col("MobilePhoneModel")), Agg("COUNT", Col("UserID"), true, "u")},
                     {Col("MobilePhoneModel")}, Cmp(Col("MobilePhoneModel"), ComparisonKind::NotEqual, Str("''")),
                     {OrderExpr(Col("u"), true)}, 10);
        case 11:
            return Q(
                {Select(Col("MobilePhone")), Select(Col("MobilePhoneModel")), Agg("COUNT", Col("UserID"), true, "u")},
                {Col("MobilePhone"), Col("MobilePhoneModel")},
                Cmp(Col("MobilePhoneModel"), ComparisonKind::NotEqual, Str("''")), {OrderExpr(Col("u"), true)}, 10);
        case 12:
            return Q({Select(Col("SearchPhrase")), Agg("COUNT", Star(), false, "c")}, {Col("SearchPhrase")},
                     Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")), {OrderExpr(Col("c"), true)}, 10);
        case 13:
            return Q({Select(Col("SearchPhrase")), Agg("COUNT", Col("UserID"), true, "u")}, {Col("SearchPhrase")},
                     Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")), {OrderExpr(Col("u"), true)}, 10);
        case 14:
            return Q({Select(Col("SearchEngineID")), Select(Col("SearchPhrase")), Agg("COUNT", Star(), false, "c")},
                     {Col("SearchEngineID"), Col("SearchPhrase")},
                     Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")), {OrderExpr(Col("c"), true)}, 10);
        case 15:
            return Q({Select(Col("UserID")), Agg("COUNT", Star())}, {Col("UserID")}, {},
                     {OrderAgg("COUNT", Star(), true)}, 10);
        case 16:
            return Q({Select(Col("UserID")), Select(Col("SearchPhrase")), Agg("COUNT", Star())},
                     {Col("UserID"), Col("SearchPhrase")}, {}, {OrderAgg("COUNT", Star(), true)}, 10);
        case 17:
            return Q({Select(Col("UserID")), Select(Col("SearchPhrase")), Agg("COUNT", Star())},
                     {Col("UserID"), Col("SearchPhrase")}, {}, {}, 10);
        case 18:
            return Q({Select(Col("UserID")), Select(ExtractMinute(Col("EventTime")), "m"), Select(Col("SearchPhrase")),
                      Agg("COUNT", Star())},
                     {Col("UserID"), Col("m"), Col("SearchPhrase")}, {}, {OrderAgg("COUNT", Star(), true)}, 10);
        case 19:
            return Q({Select(Col("UserID"))}, {}, Cmp(Col("UserID"), ComparisonKind::Equal, Num("435090932899640449")));
        case 20:
            return Q({Agg("COUNT", Star())}, {}, Like(Col("URL"), Str("'%google%'")));
        case 21:
            return Q({Select(Col("SearchPhrase")), Agg("MIN", Col("URL")), Agg("COUNT", Star(), false, "c")},
                     {Col("SearchPhrase")},
                     All({Like(Col("URL"), Str("'%google%'")),
                          Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''"))}),
                     {OrderExpr(Col("c"), true)}, 10);
        case 22:
            return Q({Select(Col("SearchPhrase")), Agg("MIN", Col("URL")), Agg("MIN", Col("Title")),
                      Agg("COUNT", Star(), false, "c"), Agg("COUNT", Col("UserID"), true)},
                     {Col("SearchPhrase")},
                     All({Like(Col("Title"), Str("'%Google%'")), Like(Col("URL"), Str("'%.google.%'"), true),
                          Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''"))}),
                     {OrderExpr(Col("c"), true)}, 10);
        case 23:
            return Q({Select(Star())}, {}, Like(Col("URL"), Str("'%google%'")), {OrderExpr(Col("EventTime"))}, 10);
        case 24:
            return Q({Select(Col("SearchPhrase"))}, {}, Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")),
                     {OrderExpr(Col("EventTime"))}, 10);
        case 25:
            return Q({Select(Col("SearchPhrase"))}, {}, Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")),
                     {OrderExpr(Col("SearchPhrase"))}, 10);
        case 26:
            return Q({Select(Col("SearchPhrase"))}, {}, Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")),
                     {OrderExpr(Col("EventTime")), OrderExpr(Col("SearchPhrase"))}, 10);
        case 27:
            return Q({Select(Col("CounterID")), Agg("AVG", Func("STRLEN", {Col("URL")}), false, "l"),
                      Agg("COUNT", Star(), false, "c")},
                     {Col("CounterID")}, Cmp(Col("URL"), ComparisonKind::NotEqual, Str("''")),
                     {OrderExpr(Col("l"), true)}, 25, 0, Cmp(Col("c"), ComparisonKind::Greater, Num("100000")));
        case 28:
            return Q({Select(Func("REGEXP_REPLACE",
                                  {Col("Referer"), Str(R"('^https?://(?:www\.)?([^/]+)/.*$')"), Str(R"('\1')")},
                                  "REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1')"),
                             "k"),
                      Agg("AVG", Func("STRLEN", {Col("Referer")}), false, "l"), Agg("COUNT", Star(), false, "c"),
                      Agg("MIN", Col("Referer"))},
                     {Col("k")}, Cmp(Col("Referer"), ComparisonKind::NotEqual, Str("''")), {OrderExpr(Col("l"), true)},
                     25, 0, Cmp(Col("c"), ComparisonKind::Greater, Num("100000")));
        case 29: {
            std::vector<SelectItemSpec> items;
            items.push_back(Agg("SUM", Col("ResolutionWidth")));
            for (int offset = 1; offset <= 89; ++offset) {
                items.push_back(Agg("SUM", Add(Col("ResolutionWidth"), Num(std::to_string(offset)))));
            }
            return Q(std::move(items));
        }
        case 30:
            return Q({Select(Col("SearchEngineID")), Select(Col("ClientIP")), Agg("COUNT", Star(), false, "c"),
                      Agg("SUM", Col("IsRefresh")), Agg("AVG", Col("ResolutionWidth"))},
                     {Col("SearchEngineID"), Col("ClientIP")},
                     Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")), {OrderExpr(Col("c"), true)}, 10);
        case 31:
            return Q({Select(Col("WatchID")), Select(Col("ClientIP")), Agg("COUNT", Star(), false, "c"),
                      Agg("SUM", Col("IsRefresh")), Agg("AVG", Col("ResolutionWidth"))},
                     {Col("WatchID"), Col("ClientIP")}, Cmp(Col("SearchPhrase"), ComparisonKind::NotEqual, Str("''")),
                     {OrderExpr(Col("c"), true)}, 10);
        case 32:
            return Q({Select(Col("WatchID")), Select(Col("ClientIP")), Agg("COUNT", Star(), false, "c"),
                      Agg("SUM", Col("IsRefresh")), Agg("AVG", Col("ResolutionWidth"))},
                     {Col("WatchID"), Col("ClientIP")}, {}, {OrderExpr(Col("c"), true)}, 10);
        case 33:
            return Q({Select(Col("URL")), Agg("COUNT", Star(), false, "c")}, {Col("URL")}, {},
                     {OrderExpr(Col("c"), true)}, 10);
        case 34:
            return Q({Select(Num("1")), Select(Col("URL")), Agg("COUNT", Star(), false, "c")}, {Num("1"), Col("URL")},
                     {}, {OrderExpr(Col("c"), true)}, 10);
        case 35:
            return Q({Select(Col("ClientIP")), Select(Sub(Col("ClientIP"), Num("1"))),
                      Select(Sub(Col("ClientIP"), Num("2"))), Select(Sub(Col("ClientIP"), Num("3"))),
                      Agg("COUNT", Star(), false, "c")},
                     {Col("ClientIP"), Sub(Col("ClientIP"), Num("1")), Sub(Col("ClientIP"), Num("2")),
                      Sub(Col("ClientIP"), Num("3"))},
                     {}, {OrderExpr(Col("c"), true)}, 10);
        case 36:
            return Q({Select(Col("URL")), Agg("COUNT", Star(), false, "PageViews")}, {Col("URL")},
                     All({Cmp(Col("CounterID"), ComparisonKind::Equal, Num("62")),
                          Cmp(Col("EventDate"), ComparisonKind::GreaterOrEqual, Str("'2013-07-01'")),
                          Cmp(Col("EventDate"), ComparisonKind::LessOrEqual, Str("'2013-07-31'")),
                          Cmp(Col("DontCountHits"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("IsRefresh"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("URL"), ComparisonKind::NotEqual, Str("''"))}),
                     {OrderExpr(Col("PageViews"), true)}, 10);
        case 37:
            return Q({Select(Col("Title")), Agg("COUNT", Star(), false, "PageViews")}, {Col("Title")},
                     All({Cmp(Col("CounterID"), ComparisonKind::Equal, Num("62")),
                          Cmp(Col("EventDate"), ComparisonKind::GreaterOrEqual, Str("'2013-07-01'")),
                          Cmp(Col("EventDate"), ComparisonKind::LessOrEqual, Str("'2013-07-31'")),
                          Cmp(Col("DontCountHits"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("IsRefresh"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("Title"), ComparisonKind::NotEqual, Str("''"))}),
                     {OrderExpr(Col("PageViews"), true)}, 10);
        case 38:
            return Q({Select(Col("URL")), Agg("COUNT", Star(), false, "PageViews")}, {Col("URL")},
                     All({Cmp(Col("CounterID"), ComparisonKind::Equal, Num("62")),
                          Cmp(Col("EventDate"), ComparisonKind::GreaterOrEqual, Str("'2013-07-01'")),
                          Cmp(Col("EventDate"), ComparisonKind::LessOrEqual, Str("'2013-07-31'")),
                          Cmp(Col("IsRefresh"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("IsLink"), ComparisonKind::NotEqual, Num("0")),
                          Cmp(Col("IsDownload"), ComparisonKind::Equal, Num("0"))}),
                     {OrderExpr(Col("PageViews"), true)}, 10, 1000);
        case 39:
            return Q({Select(Col("TraficSourceID")), Select(Col("SearchEngineID")), Select(Col("AdvEngineID")),
                      Select(Case(All({Cmp(Col("SearchEngineID"), ComparisonKind::Equal, Num("0")),
                                       Cmp(Col("AdvEngineID"), ComparisonKind::Equal, Num("0"))}),
                                  Col("Referer"), Str("''")),
                             "Src"),
                      Select(Col("URL"), "Dst"), Agg("COUNT", Star(), false, "PageViews")},
                     {Col("TraficSourceID"), Col("SearchEngineID"), Col("AdvEngineID"), Col("Src"), Col("Dst")},
                     All({Cmp(Col("CounterID"), ComparisonKind::Equal, Num("62")),
                          Cmp(Col("EventDate"), ComparisonKind::GreaterOrEqual, Str("'2013-07-01'")),
                          Cmp(Col("EventDate"), ComparisonKind::LessOrEqual, Str("'2013-07-31'")),
                          Cmp(Col("IsRefresh"), ComparisonKind::Equal, Num("0"))}),
                     {OrderExpr(Col("PageViews"), true)}, 10, 1000);
        case 40:
            return Q({Select(Col("URLHash")), Select(Col("EventDate")), Agg("COUNT", Star(), false, "PageViews")},
                     {Col("URLHash"), Col("EventDate")},
                     All({Cmp(Col("CounterID"), ComparisonKind::Equal, Num("62")),
                          Cmp(Col("EventDate"), ComparisonKind::GreaterOrEqual, Str("'2013-07-01'")),
                          Cmp(Col("EventDate"), ComparisonKind::LessOrEqual, Str("'2013-07-31'")),
                          Cmp(Col("IsRefresh"), ComparisonKind::Equal, Num("0")),
                          In(Col("TraficSourceID"), {Num("-1"), Num("6")}),
                          Cmp(Col("RefererHash"), ComparisonKind::Equal, Num("3594120000172545465"))}),
                     {OrderExpr(Col("PageViews"), true)}, 10, 100);
        case 41:
            return Q({Select(Col("WindowClientWidth")), Select(Col("WindowClientHeight")),
                      Agg("COUNT", Star(), false, "PageViews")},
                     {Col("WindowClientWidth"), Col("WindowClientHeight")},
                     All({Cmp(Col("CounterID"), ComparisonKind::Equal, Num("62")),
                          Cmp(Col("EventDate"), ComparisonKind::GreaterOrEqual, Str("'2013-07-01'")),
                          Cmp(Col("EventDate"), ComparisonKind::LessOrEqual, Str("'2013-07-31'")),
                          Cmp(Col("IsRefresh"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("DontCountHits"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("URLHash"), ComparisonKind::Equal, Num("2868770270353813622"))}),
                     {OrderExpr(Col("PageViews"), true)}, 10, 10000);
        case 42:
            return Q({Select(DateTruncMinute(Col("EventTime")), "M"), Agg("COUNT", Star(), false, "PageViews")},
                     {Col("M")},
                     All({Cmp(Col("CounterID"), ComparisonKind::Equal, Num("62")),
                          Cmp(Col("EventDate"), ComparisonKind::GreaterOrEqual, Str("'2013-07-14'")),
                          Cmp(Col("EventDate"), ComparisonKind::LessOrEqual, Str("'2013-07-15'")),
                          Cmp(Col("IsRefresh"), ComparisonKind::Equal, Num("0")),
                          Cmp(Col("DontCountHits"), ComparisonKind::Equal, Num("0"))}),
                     {OrderExpr(Col("M"))}, 10, 1000);
        default:
            return std::nullopt;
    }
}

}  // namespace clickbench

namespace clickbench_fast {

struct Int128Hash {
    size_t operator()(const Int128 value) const noexcept {
        const UInt128 raw = static_cast<UInt128>(value);
        const uint64_t low = static_cast<uint64_t>(raw);
        const uint64_t high = static_cast<uint64_t>(raw >> 64);
        return std::hash<uint64_t>{}(low ^ (high + 0x9e3779b97f4a7c15ULL + (low << 6) + (low >> 2)));
    }
};

struct Int128PairHash {
    size_t operator()(const std::pair<Int128, Int128>& value) const noexcept {
        const size_t lhs = Int128Hash{}(value.first);
        const size_t rhs = Int128Hash{}(value.second);
        return lhs ^ (rhs + 0x9e3779b97f4a7c15ULL + (lhs << 6) + (lhs >> 2));
    }
};

inline Batch SingleRow(const Schema& schema, const std::initializer_list<std::string_view> values) {
    Batch batch(schema, 1);
    size_t column = 0;

    for (const std::string_view value : values) {
        batch.AppendValueFromString(column++, value);
    }

    return batch;
}

inline std::unique_ptr<Operator> ScanColumns(const std::filesystem::path& path, const Schema& schema,
                                             const std::initializer_list<std::string_view> columns) {
    std::vector<size_t> projection_indexes;
    projection_indexes.reserve(columns.size());

    for (const std::string_view column : columns) {
        projection_indexes.push_back(FindColumnIndex(schema, column));
    }

    return CreateScanOperator(path, std::move(projection_indexes), {});
}

inline ExecuteExpected CountAll(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    uint64_t rows = 0;

    for (const auto& row_group : reader.GetMetadata().row_groups) {
        rows += row_group.row_count;
    }

    return SingleRow(Schema{{ColumnSchema("COUNT(*)", ColumnType::Int64)}}, {std::to_string(rows)});
}

inline ExecuteExpected CountAdvEngineNonZero(const std::filesystem::path& path) {
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

inline ExecuteExpected SumCountAvg(const std::filesystem::path& path) {
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

inline ExecuteExpected AvgUserId(const std::filesystem::path& path) {
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

inline ExecuteExpected CountDistinctColumn(const std::filesystem::path& path, const std::string_view column_name,
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

inline ExecuteExpected MinMaxEventDate(const std::filesystem::path& path) {
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

inline ExecuteExpected CountUrlContainsGoogle(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    const auto scan = ScanColumns(path, reader.GetSchema(), {"URL"});

    uint64_t count = 0;

    while (const auto batch = scan->Next()) {
        const Column& url = batch->ColumnAt(0);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (url.ValueAsString(row).find("google") != std::string::npos) {
                ++count;
            }
        }
    }

    return SingleRow(Schema{{ColumnSchema("COUNT(*)", ColumnType::Int64)}}, {std::to_string(count)});
}

inline bool Contains(const std::string_view haystack, const std::string_view needle) {
    return haystack.find(needle) != std::string_view::npos;
}

inline bool IsEmptyString(const std::string_view value) { return value.empty(); }

inline std::string ExtractRefererHost(std::string_view referer) {
    if (referer.empty()) {
        return {};
    }

    if (referer.rfind("http://", 0) == 0) {
        referer.remove_prefix(7);
    } else if (referer.rfind("https://", 0) == 0) {
        referer.remove_prefix(8);
    }

    if (referer.rfind("www.", 0) == 0) {
        referer.remove_prefix(4);
    }

    const size_t slash = referer.find('/');

    return std::string(referer.substr(0, slash));
}

struct SearchPhraseGroup {
    std::string_view key;
    uint64_t count = 0;
    std::string_view min_url;
    std::string_view min_title;
    std::unordered_set<Int128, Int128Hash> distinct_users;
    size_t ordinal = 0;
    bool has_min_url = false;
    bool has_min_title = false;
};

inline ExecuteExpected SearchPhraseGroupByUrlContainsGoogle(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    auto scan = ScanColumns(path, reader.GetSchema(), {"URL", "SearchPhrase"});

    StringArena arena;
    std::unordered_map<std::string_view, SearchPhraseGroup, StringViewHash, StringViewEqual> groups;
    size_t ordinal = 0;

    while (auto batch = scan->Next()) {
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
                group.has_min_url = true;
            } else if (url.ValueAsString(row) < group.min_url) {
                group.min_url = arena.Store(url.ValueAsString(row));
            }

            ++group.count;
        }
    }

    std::vector<const SearchPhraseGroup*> ordered;
    ordered.reserve(groups.size());

    for (const auto& val : groups | std::views::values) {
        ordered.push_back(&val);
    }

    std::ranges::sort(ordered, [](const SearchPhraseGroup* lhs, const SearchPhraseGroup* rhs) {
        if (lhs->count != rhs->count) {
            return lhs->count > rhs->count;
        }
        return lhs->ordinal < rhs->ordinal;
    });

    if (ordered.size() > 10) {
        ordered.resize(10);
    }

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

struct SearchPhraseStats {
    std::string_view key;
    uint64_t count = 0;
    std::string_view min_url;
    std::string_view min_title;
    std::unordered_set<Int128, Int128Hash> distinct_users;
    size_t ordinal = 0;
};

inline ExecuteExpected SearchPhraseGroupByTitleGoogle(const std::filesystem::path& path) {
    const ColumnarBatchReader reader(path);
    auto scan = ScanColumns(path, reader.GetSchema(), {"Title", "URL", "SearchPhrase", "UserID"});

    StringArena arena;
    std::unordered_map<std::string_view, SearchPhraseStats, StringViewHash, StringViewEqual> groups;
    size_t ordinal = 0;

    while (auto batch = scan->Next()) {
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

    std::vector<const SearchPhraseStats*> ordered;
    ordered.reserve(groups.size());

    for (const auto& val : groups | std::views::values) {
        ordered.push_back(&val);
    }

    std::ranges::sort(ordered, [](const SearchPhraseStats* lhs, const SearchPhraseStats* rhs) {
        if (lhs->count != rhs->count) {
            return lhs->count > rhs->count;
        }
        return lhs->ordinal < rhs->ordinal;
    });

    if (ordered.size() > 10) {
        ordered.resize(10);
    }

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

struct IntGroupStats {
    Int128 key1 = 0;
    Int128 key2 = 0;
    uint64_t count = 0;
    Int128 sum_refresh = 0;
    Int128 sum_width = 0;
    size_t ordinal = 0;
};

template <typename Map>
std::vector<const typename Map::mapped_type*> OrderGroupsByCount(const Map& groups, const size_t limit) {
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

inline ExecuteExpected SumWidthVariants(const std::filesystem::path& path) {
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

inline ExecuteExpected GroupBySearchEngineAndClientIP(const std::filesystem::path& path,
                                                      const bool filter_search_phrase,
                                                      const std::string_view filter_value,
                                                      const std::string_view output_group_name) {
    const ColumnarBatchReader reader(path);
    auto scan = ScanColumns(path, reader.GetSchema(),
                            {"SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"});

    std::unordered_map<std::pair<Int128, Int128>, IntGroupStats, Int128PairHash> groups;
    size_t ordinal = 0;

    while (auto batch = scan->Next()) {
        const Column& search_engine = batch->ColumnAt(0);
        const Column& client_ip = batch->ColumnAt(1);
        const Column& is_refresh = batch->ColumnAt(2);
        const Column& width = batch->ColumnAt(3);
        const Column& search_phrase = batch->ColumnAt(4);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (filter_search_phrase && search_phrase.ValueAsString(row).empty() == false &&
                search_phrase.ValueAsString(row) != filter_value) {
                continue;
            }
            if (filter_search_phrase && search_phrase.ValueAsString(row).empty()) {
                continue;
            }

            const std::pair<Int128, Int128> key{search_engine.ValueAsInt128(row), client_ip.ValueAsInt128(row)};
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
    Batch result(
        Schema{{ColumnSchema(std::string(output_group_name) + "1", ColumnType::Int128),
                ColumnSchema(std::string(output_group_name) + "2", ColumnType::Int128),
                ColumnSchema("COUNT(*)", ColumnType::Int64), ColumnSchema("SUM(IsRefresh)", ColumnType::Int128),
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

inline ExecuteExpected GroupByURLCount(const std::filesystem::path& path, const bool add_constant_column) {
    const ColumnarBatchReader reader(path);
    auto scan = ScanColumns(path, reader.GetSchema(), {"URL"});

    StringArena arena;
    std::unordered_map<std::string_view, SearchPhraseStats, StringViewHash, StringViewEqual> groups;
    size_t ordinal = 0;

    while (auto batch = scan->Next()) {
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

inline ExecuteExpected GroupBySearchEngineClientIP(const std::filesystem::path& path, const bool filter_search_phrase) {
    const ColumnarBatchReader reader(path);
    auto scan = ScanColumns(path, reader.GetSchema(),
                            {"SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"});

    std::unordered_map<std::pair<Int128, Int128>, IntGroupStats, Int128PairHash> groups;
    size_t ordinal = 0;

    while (auto batch = scan->Next()) {
        const Column& search_engine = batch->ColumnAt(0);
        const Column& client_ip = batch->ColumnAt(1);
        const Column& is_refresh = batch->ColumnAt(2);
        const Column& width = batch->ColumnAt(3);
        const Column& search_phrase = batch->ColumnAt(4);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (filter_search_phrase && search_phrase.ValueAsString(row).empty()) {
                continue;
            }

            const std::pair<Int128, Int128> key{search_engine.ValueAsInt128(row), client_ip.ValueAsInt128(row)};
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
    Batch result(
        Schema{{ColumnSchema("SearchEngineID", ColumnType::Int128), ColumnSchema("ClientIP", ColumnType::Int128),
                ColumnSchema("c", ColumnType::Int64), ColumnSchema("SUM(IsRefresh)", ColumnType::Int128),
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

inline ExecuteExpected GroupByWatchClientIP(const std::filesystem::path& path, const bool filter_search_phrase) {
    const ColumnarBatchReader reader(path);
    auto scan =
        ScanColumns(path, reader.GetSchema(), {"WatchID", "ClientIP", "IsRefresh", "ResolutionWidth", "SearchPhrase"});

    std::unordered_map<std::pair<Int128, Int128>, IntGroupStats, Int128PairHash> groups;
    size_t ordinal = 0;

    while (auto batch = scan->Next()) {
        const Column& watch_id = batch->ColumnAt(0);
        const Column& client_ip = batch->ColumnAt(1);
        const Column& is_refresh = batch->ColumnAt(2);
        const Column& width = batch->ColumnAt(3);
        const Column& search_phrase = batch->ColumnAt(4);
        for (size_t row = 0; row < batch->RowsCount(); ++row) {
            if (filter_search_phrase && search_phrase.ValueAsString(row).empty()) {
                continue;
            }

            const std::pair<Int128, Int128> key{watch_id.ValueAsInt128(row), client_ip.ValueAsInt128(row)};
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
    Batch result(Schema{{ColumnSchema("WatchID", ColumnType::Int128), ColumnSchema("ClientIP", ColumnType::Int128),
                         ColumnSchema("c", ColumnType::Int64), ColumnSchema("SUM(IsRefresh)", ColumnType::Int128),
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

inline ExecuteExpected GroupByClientIPDerived(const std::filesystem::path& path) {
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

struct SearchPhraseOrderRow {
    Int128 event_time = 0;
    std::string_view search_phrase;
    size_t ordinal = 0;
};

inline ExecuteExpected SelectSearchPhraseOrderByEventTime(const std::filesystem::path& path) {
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

inline ExecuteExpected SelectSearchPhraseOrderByPhrase(const std::filesystem::path& path) {
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

inline ExecuteExpected SelectSearchPhraseOrderByEventTimeThenPhrase(const std::filesystem::path& path) {
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

inline std::optional<ExecuteExpected> TryRun(const std::filesystem::path& path, const int query_id) {
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
            return GroupBySearchEngineClientIP(path, true);
        case 31:
            return GroupByWatchClientIP(path, true);
        case 32:
            return GroupByWatchClientIP(path, false);
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

inline bool HasHardcodedClickBenchQuery(const int query_id) { return query_id >= 0 && query_id <= 42; }

inline Query BuildRequiredClickBenchQuery(const int query_id) {
    auto query = clickbench::BuildClickBenchQuery(query_id);
    if (!query.has_value()) {
        throw Error::InvalidArgument("benchmark", "unknown ClickBench query id");
    }
    return std::move(*query);
}

inline ExecuteExpected RunHardcodedClickBenchQuery(const Executor& executor, const int query_id) {
    return executor.Execute(BuildRequiredClickBenchQuery(query_id));
}
