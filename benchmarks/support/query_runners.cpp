#include "query_runners.h"

#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/error.h"

static ExprPtr MakeExpr(const ExprKind kind, std::string output_name = {}) {
    auto expr = std::make_shared<ExprSpec>();
    expr->kind = kind;
    expr->output_name = std::move(output_name);
    return expr;
}

static ExprPtr Col(std::string name) {
    auto expr = MakeExpr(ExprKind::Column, name);
    expr->column = ColumnRef{.qualifier = {}, .name = std::move(name)};
    return expr;
}

static ExprPtr Num(std::string text) {
    auto expr = MakeExpr(ExprKind::Literal, text);
    expr->literal = QueryLiteral{.text = std::move(text), .kind = LiteralKind::Numeric};
    return expr;
}

static ExprPtr Str(std::string text) {
    auto expr = MakeExpr(ExprKind::Literal, text);
    expr->literal = QueryLiteral{.text = std::move(text), .kind = LiteralKind::String};
    return expr;
}

static ExprPtr Ident(std::string text) {
    auto expr = MakeExpr(ExprKind::Literal, text);
    expr->literal = QueryLiteral{.text = std::move(text), .kind = LiteralKind::Identifier};
    return expr;
}

static ExprPtr Star() { return MakeExpr(ExprKind::Star, "*"); }

static std::string FormatFunctionName(const std::string_view function, const std::vector<ExprPtr>& args) {
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

static ExprPtr Func(std::string name, std::vector<ExprPtr> args, std::string output_name = {}) {
    auto expr = MakeExpr(ExprKind::Function);
    expr->function_name = std::move(name);
    expr->arguments = std::move(args);
    expr->output_name =
        output_name.empty() ? FormatFunctionName(expr->function_name, expr->arguments) : std::move(output_name);
    return expr;
}

static ExprPtr ExtractMinute(ExprPtr expr) {
    return Func("EXTRACT", {Ident("MINUTE"), std::move(expr)}, "EXTRACT(MINUTE, EventTime)");
}

static ExprPtr DateTruncMinute(ExprPtr expr) {
    return Func("DATE_TRUNC", {Str("'minute'"), std::move(expr)}, "DATE_TRUNC('minute', EventTime)");
}

static ExprPtr Binary(ExprPtr left, const BinaryOp op, ExprPtr right) {
    auto expr = MakeExpr(ExprKind::Binary);
    expr->binary_op = op;
    expr->output_name = left->output_name + (op == BinaryOp::Add ? " + " : " - ") + right->output_name;
    expr->left = std::move(left);
    expr->right = std::move(right);
    return expr;
}

static ExprPtr Add(ExprPtr left, ExprPtr right) { return Binary(std::move(left), BinaryOp::Add, std::move(right)); }

static ExprPtr Sub(ExprPtr left, ExprPtr right) {
    return Binary(std::move(left), BinaryOp::Subtract, std::move(right));
}

static ExprPtr Case(PredicatePtr condition, ExprPtr then_expr, ExprPtr else_expr) {
    auto expr = MakeExpr(ExprKind::Case, "CASE");
    expr->case_spec = CaseSpec{
        .condition = std::move(condition),
        .then_expr = std::move(then_expr),
        .else_expr = std::move(else_expr),
    };
    return expr;
}

static PredicatePtr Pred(const PredicateKind kind) {
    auto pred = std::make_shared<PredicateSpec>();
    pred->kind = kind;
    return pred;
}

static PredicatePtr Cmp(ExprPtr left, const ComparisonKind comparison, ExprPtr right) {
    auto pred = Pred(PredicateKind::Comparison);
    pred->left = std::move(left);
    pred->comparison = comparison;
    pred->right = std::move(right);
    return pred;
}

static PredicatePtr Like(ExprPtr left, ExprPtr pattern, const bool negated = false) {
    auto pred = Pred(negated ? PredicateKind::NotLike : PredicateKind::Like);
    pred->left = std::move(left);
    pred->right = std::move(pattern);
    return pred;
}

static PredicatePtr In(ExprPtr left, std::vector<ExprPtr> values) {
    auto pred = Pred(PredicateKind::In);
    pred->left = std::move(left);
    pred->values = std::move(values);
    return pred;
}

static PredicatePtr And(PredicatePtr lhs, PredicatePtr rhs) {
    auto pred = Pred(PredicateKind::And);
    pred->lhs = std::move(lhs);
    pred->rhs = std::move(rhs);
    return pred;
}

static PredicatePtr All(const std::initializer_list<PredicatePtr> predicates) {
    auto it = predicates.begin();
    PredicatePtr result = *it++;
    for (; it != predicates.end(); ++it) {
        result = And(std::move(result), *it);
    }
    return result;
}

static SelectItemSpec Select(ExprPtr expr, std::string alias = {}) {
    SelectItemSpec item;
    item.kind = SelectItemKind::GroupKey;
    item.column = expr->kind == ExprKind::Column ? expr->column : ColumnRef{};
    item.expression = std::move(expr);
    item.output_name = alias.empty() ? item.expression->output_name : std::move(alias);
    return item;
}

static SelectItemSpec Agg(std::string function, ExprPtr argument, const bool distinct = false, std::string alias = {}) {
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

static OrderBySpec Order(SelectItemSpec item, const bool descending = false) {
    return OrderBySpec{.item = std::move(item), .descending = descending};
}

static OrderBySpec OrderExpr(ExprPtr expr, const bool descending = false) {
    return Order(Select(std::move(expr)), descending);
}

static OrderBySpec OrderAgg(std::string function, ExprPtr argument, const bool descending = false) {
    return Order(Agg(std::move(function), std::move(argument)), descending);
}

static Query Q(std::vector<SelectItemSpec> select_items, std::vector<ExprPtr> group_by = {}, PredicatePtr filter = {},
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

static std::optional<Query> BuildClickBenchQuery(const int id) {
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

bool HasHardcodedClickBenchQuery(const int query_id) { return query_id >= 0 && query_id <= 42; }

Query BuildRequiredClickBenchQuery(const int query_id) {
    auto query = BuildClickBenchQuery(query_id);
    if (!query.has_value()) {
        throw Error::InvalidArgument("benchmark", "unknown ClickBench query id");
    }
    return std::move(*query);
}

ExecuteExpected RunHardcodedClickBenchQuery(const Executor& executor, const int query_id) {
    return executor.Execute(BuildRequiredClickBenchQuery(query_id));
}
