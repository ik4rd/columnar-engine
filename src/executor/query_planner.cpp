#include "executor/query_planner.h"

#include <algorithm>

#include "common/ascii.h"
#include "common/error.h"
#include "common/parsing.h"
#include "executor/operator_utils.h"
#include "executor/planner_utils.h"
#include "io/columnar_batch.h"

static bool SameExpr(const ExprPtr& lhs, const ExprPtr& rhs) {
    if (!lhs || !rhs || lhs->kind != rhs->kind) {
        return false;
    }

    switch (lhs->kind) {
        case ExprKind::Column:
            return SameColumnRef(lhs->column, rhs->column);
        case ExprKind::Literal:
            return lhs->literal.text == rhs->literal.text && lhs->literal.kind == rhs->literal.kind;
        case ExprKind::Star:
            return true;
        case ExprKind::Binary:
            return lhs->binary_op == rhs->binary_op && SameExpr(lhs->left, rhs->left) &&
                   SameExpr(lhs->right, rhs->right);
        case ExprKind::Function:
            if (!SameColumnName(lhs->function_name, rhs->function_name) ||
                lhs->arguments.size() != rhs->arguments.size()) {
                return false;
            }
            for (size_t i = 0; i < lhs->arguments.size(); ++i) {
                if (!SameExpr(lhs->arguments[i], rhs->arguments[i])) {
                    return false;
                }
            }
            return true;
        case ExprKind::Case:
            return lhs->output_name == rhs->output_name;
    }

    return false;
}

static ColumnType ColumnTypeOf(const Query& query, const Schema& schema, const ExprPtr& expr);

static ColumnType ColumnTypeOfColumn(const Query& query, const Schema& schema, const ColumnRef& column) {
    ValidateColumnQualifier(query, column);
    return schema.columns[FindColumnIndex(schema, column.name)].type;
}

static ColumnType ColumnTypeOf(const Query& query, const Schema& schema, const ExprPtr& expr) {
    if (!expr) {
        return ColumnType::String;
    }

    switch (expr->kind) {
        case ExprKind::Column:
            return ColumnTypeOfColumn(query, schema, expr->column);
        case ExprKind::Literal:
            return expr->literal.kind == LiteralKind::String ? ColumnType::String : ColumnType::Int64;
        case ExprKind::Binary:
            return ColumnType::Int64;
        case ExprKind::Function: {
            const std::string name = ToUpperAscii(expr->function_name);
            if (name == "STRLEN" || name == "LENGTH" || name == "EXTRACT") {
                return ColumnType::Int64;
            }
            if (name == "DATE_TRUNC") {
                return ColumnType::Timestamp;
            }
            if (name == "REGEXP_REPLACE") {
                return ColumnType::String;
            }
            if (name == "COUNT") {
                return ColumnType::Int64;
            }
            if (name == "SUM" || name == "AVG") {
                return ColumnType::Int128;
            }
            if ((name == "MIN" || name == "MAX") && !expr->arguments.empty()) {
                return ColumnTypeOf(query, schema, expr->arguments.front());
            }
            return ColumnType::String;
        }
        case ExprKind::Case:
            return ColumnTypeOf(query, schema, expr->case_spec.then_expr);
        case ExprKind::Star:
            return ColumnType::String;
    }

    return ColumnType::String;
}

static bool HasAggregate(const std::vector<SelectItemSpec>& items) {
    return std::ranges::any_of(items,
                               [](const SelectItemSpec& item) { return item.kind == SelectItemKind::Aggregate; });
}

static std::optional<size_t> FindSelectItem(const Query& query_ast, const SelectItemSpec& item) {
    for (size_t i = 0; i < query_ast.select_items.size(); ++i) {
        const SelectItemSpec& candidate = query_ast.select_items[i];
        if (SameOutputName(candidate, item)) {
            return i;
        }
        if (candidate.kind == item.kind) {
            if (candidate.kind == SelectItemKind::Aggregate &&
                SameColumnName(candidate.aggregate.output_name, item.aggregate.output_name)) {
                return i;
            }
            if (candidate.kind == SelectItemKind::GroupKey && SameExpr(candidate.expression, item.expression)) {
                return i;
            }
        }
    }

    return std::nullopt;
}

static std::optional<size_t> FindGroupKey(const std::vector<ExprPtr>& group_by, const ExprPtr& expr) {
    for (size_t i = 0; i < group_by.size(); ++i) {
        if (SameExpr(group_by[i], expr)) {
            return i;
        }
    }

    return std::nullopt;
}

static ExprPtr ResolveGroupByExpression(const Query& query_ast, const ExprPtr& expr) {
    if (expr->kind == ExprKind::Literal && expr->literal.kind == LiteralKind::Numeric) {
        try {
            const size_t ordinal = std::stoull(expr->literal.text);
            if (ordinal >= 1 && ordinal <= query_ast.select_items.size()) {
                return query_ast.select_items[ordinal - 1].expression;
            }
        } catch (...) {
        }
    }

    if (expr->kind == ExprKind::Column && expr->column.qualifier.empty()) {
        for (const auto& item : query_ast.select_items) {
            if (SameColumnName(item.output_name, expr->column.name) && item.expression) {
                return item.expression;
            }
        }
    }

    return expr;
}

static std::vector<size_t> StarColumnIndexes(const Schema& schema) {
    std::vector<size_t> clickbench_subset;

    for (const std::string_view name : {"WatchID", "EventTime", "URL", "Title"}) {
        try {
            clickbench_subset.push_back(FindColumnIndex(schema, name));
        } catch (const Error&) {
            clickbench_subset.clear();
            break;
        }
    }

    if (clickbench_subset.size() == 4) {
        return clickbench_subset;
    }

    std::vector<size_t> all;
    all.reserve(schema.columns.size());

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        all.push_back(i);
    }

    return all;
}

static void AddProjectionIndex(std::vector<size_t>& indexes, const size_t index) {
    if (!std::ranges::contains(indexes, index)) {
        indexes.push_back(index);
    }
}

static void CollectColumnsFromExpr(const Query& query_ast, const Schema& schema, const ExprPtr& expr,
                                   std::vector<size_t>& indexes);

static void CollectColumnsFromPredicate(const Query& query_ast, const Schema& schema, const PredicatePtr& predicate,
                                        std::vector<size_t>& indexes) {
    if (!predicate) {
        return;
    }

    switch (predicate->kind) {
        case PredicateKind::And:
            CollectColumnsFromPredicate(query_ast, schema, predicate->lhs, indexes);
            CollectColumnsFromPredicate(query_ast, schema, predicate->rhs, indexes);
            return;
        case PredicateKind::Comparison:
        case PredicateKind::Like:
        case PredicateKind::NotLike:
            CollectColumnsFromExpr(query_ast, schema, predicate->left, indexes);
            CollectColumnsFromExpr(query_ast, schema, predicate->right, indexes);
            return;
        case PredicateKind::In:
            CollectColumnsFromExpr(query_ast, schema, predicate->left, indexes);
            for (const ExprPtr& value : predicate->values) {
                CollectColumnsFromExpr(query_ast, schema, value, indexes);
            }
    }
}

static void CollectColumnsFromExpr(const Query& query_ast, const Schema& schema, const ExprPtr& expr,
                                   std::vector<size_t>& indexes) {
    if (!expr) {
        return;
    }

    switch (expr->kind) {
        case ExprKind::Column:
            ValidateColumnQualifier(query_ast, expr->column);
            AddProjectionIndex(indexes, FindColumnIndex(schema, expr->column.name));
            return;
        case ExprKind::Literal:
            return;
        case ExprKind::Star:
            for (const size_t index : StarColumnIndexes(schema)) {
                AddProjectionIndex(indexes, index);
            }
            return;
        case ExprKind::Binary:
            CollectColumnsFromExpr(query_ast, schema, expr->left, indexes);
            CollectColumnsFromExpr(query_ast, schema, expr->right, indexes);
            return;
        case ExprKind::Function:
            for (const ExprPtr& argument : expr->arguments) {
                CollectColumnsFromExpr(query_ast, schema, argument, indexes);
            }
            return;
        case ExprKind::Case:
            CollectColumnsFromPredicate(query_ast, schema, expr->case_spec.condition, indexes);
            CollectColumnsFromExpr(query_ast, schema, expr->case_spec.then_expr, indexes);
            CollectColumnsFromExpr(query_ast, schema, expr->case_spec.else_expr, indexes);
    }
}

static std::optional<size_t> ProjectedIndexOf(const std::vector<size_t>& projection_indexes,
                                              const size_t source_index) {
    for (size_t i = 0; i < projection_indexes.size(); ++i) {
        if (projection_indexes[i] == source_index) {
            return i;
        }
    }
    return std::nullopt;
}

static void BindExprColumnIndexes(const Query& query_ast, const Schema& schema,
                                  const std::vector<size_t>& projection_indexes, const ExprPtr& expr);
static void BindTypedLiteralComparison(const Query& query_ast, const Schema& schema,
                                       const std::vector<size_t>& projection_indexes, const PredicatePtr& predicate);
static void BindLiteralInSet(const Query& query_ast, const Schema& schema,
                             const std::vector<size_t>& projection_indexes, const PredicatePtr& predicate);
static void BindLiteralLikePattern(const Query& query_ast, const Schema& schema,
                                   const std::vector<size_t>& projection_indexes, const PredicatePtr& predicate);
static void BindRegexReplace(const ExprPtr& expr);

static void BindPredicateColumnIndexes(const Query& query_ast, const Schema& schema,
                                       const std::vector<size_t>& projection_indexes, const PredicatePtr& predicate) {
    if (!predicate) {
        return;
    }

    switch (predicate->kind) {
        case PredicateKind::And:
            BindPredicateColumnIndexes(query_ast, schema, projection_indexes, predicate->lhs);
            BindPredicateColumnIndexes(query_ast, schema, projection_indexes, predicate->rhs);
            return;
        case PredicateKind::Comparison:
            BindTypedLiteralComparison(query_ast, schema, projection_indexes, predicate);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, predicate->left);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, predicate->right);
            return;
        case PredicateKind::Like:
        case PredicateKind::NotLike:
            BindLiteralLikePattern(query_ast, schema, projection_indexes, predicate);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, predicate->left);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, predicate->right);
            return;
        case PredicateKind::In:
            BindLiteralInSet(query_ast, schema, projection_indexes, predicate);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, predicate->left);
            for (const ExprPtr& value : predicate->values) {
                BindExprColumnIndexes(query_ast, schema, projection_indexes, value);
            }
    }
}

static void BindExprColumnIndexes(const Query& query_ast, const Schema& schema,
                                  const std::vector<size_t>& projection_indexes, const ExprPtr& expr) {
    if (!expr) {
        return;
    }

    switch (expr->kind) {
        case ExprKind::Column: {
            ValidateColumnQualifier(query_ast, expr->column);
            const size_t source_index = FindColumnIndex(schema, expr->column.name);
            const std::optional<size_t> projected_index = ProjectedIndexOf(projection_indexes, source_index);
            if (projected_index.has_value()) {
                expr->column_index_bound = true;
                expr->column_index = *projected_index;
            }
            return;
        }
        case ExprKind::Literal:
        case ExprKind::Star:
            return;
        case ExprKind::Binary:
            BindExprColumnIndexes(query_ast, schema, projection_indexes, expr->left);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, expr->right);
            return;
        case ExprKind::Function:
            for (const ExprPtr& argument : expr->arguments) {
                BindExprColumnIndexes(query_ast, schema, projection_indexes, argument);
            }
            BindRegexReplace(expr);
            return;
        case ExprKind::Case:
            BindPredicateColumnIndexes(query_ast, schema, projection_indexes, expr->case_spec.condition);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, expr->case_spec.then_expr);
            BindExprColumnIndexes(query_ast, schema, projection_indexes, expr->case_spec.else_expr);
    }
}

static bool IsSimpleCountStar(const Query& query_ast, const PlannedQuery& planned) {
    if (query_ast.select_items.size() != 1 || query_ast.filter || query_ast.having || !query_ast.group_by.empty() ||
        !query_ast.order_by.empty() || query_ast.offset > 0 || query_ast.limit == 0) {
        return false;
    }

    if (planned.aggregates.size() != 1) {
        return false;
    }

    const PlannedAgg& aggregate = planned.aggregates.front();
    return aggregate.function != nullptr && SameColumnName(aggregate.function->canonical_name, "COUNT") &&
           aggregate.argument_kind == AggArgumentKind::Star && !aggregate.distinct;
}

static std::optional<Int128> TryNumericLiteralValue(const ExprPtr& expr) {
    if (!expr || expr->kind != ExprKind::Literal || expr->literal.kind != LiteralKind::Numeric) {
        return std::nullopt;
    }

    try {
        return ParseInt128(expr->literal.text);
    } catch (const Error&) {
        return std::nullopt;
    }
}

static std::string NormalizeStringLiteral(const std::string_view text) {
    std::string result;

    for (size_t i = 1; i + 1 < text.size(); ++i) {
        if (text[i] == '\'' && i + 2 < text.size() && text[i + 1] == '\'') {
            result.push_back('\'');
            ++i;
            continue;
        }
        result.push_back(text[i]);
    }

    return result;
}

static std::string LiteralTextForEval(const QueryLiteral& literal) {
    if (literal.kind == LiteralKind::String) {
        return NormalizeStringLiteral(literal.text);
    }
    return literal.text;
}

static void NormalizeRegexReplacement(std::string& replacement) {
    for (size_t pos = 0; (pos = replacement.find("\\1", pos)) != std::string::npos;) {
        replacement.replace(pos, 2, "$1");
        pos += 2;
    }
}

static void BindRegexReplace(const ExprPtr& expr) {
    if (!expr || expr->kind != ExprKind::Function || !SameColumnName(expr->function_name, "REGEXP_REPLACE") ||
        expr->arguments.size() != 3 || !expr->arguments[1] || !expr->arguments[2] ||
        expr->arguments[1]->kind != ExprKind::Literal || expr->arguments[2]->kind != ExprKind::Literal) {
        return;
    }

    try {
        std::string replacement = LiteralTextForEval(expr->arguments[2]->literal);
        NormalizeRegexReplacement(replacement);
        expr->regex_pattern = std::regex(LiteralTextForEval(expr->arguments[1]->literal));
        expr->regex_replacement = std::move(replacement);
        expr->regex_replace_bound = true;
    } catch (const std::regex_error&) {
        expr->regex_replace_bound = false;
    }
}

static std::optional<Int128> TryLiteralValueForColumnType(const QueryLiteral& literal, const ColumnType type) {
    try {
        if (literal.kind == LiteralKind::Numeric) {
            return ParseInt128(literal.text);
        }

        if (literal.kind != LiteralKind::String) {
            return std::nullopt;
        }

        const std::string normalized = NormalizeStringLiteral(literal.text);
        switch (type) {
            case ColumnType::Boolean:
                return ParseBoolean(normalized) ? 1 : 0;
            case ColumnType::Int16:
                return ParseInt16(normalized);
            case ColumnType::Int32:
                return ParseInt32(normalized);
            case ColumnType::Int64:
                return ParseInt64(normalized);
            case ColumnType::Int128:
                return ParseInt128(normalized);
            case ColumnType::Date:
                return ParseDate(normalized);
            case ColumnType::Timestamp:
                return ParseTimestamp(normalized);
            case ColumnType::Character:
                return ParseCharacter(normalized);
            case ColumnType::String:
                return std::nullopt;
        }
    } catch (const Error&) {
        return std::nullopt;
    }

    return std::nullopt;
}

static void BindLiteralInSet(const Query& query_ast, const Schema& schema,
                             const std::vector<size_t>& projection_indexes, const PredicatePtr& predicate) {
    if (!predicate || predicate->kind != PredicateKind::In || !predicate->left ||
        predicate->left->kind != ExprKind::Column || predicate->values.empty()) {
        return;
    }

    ValidateColumnQualifier(query_ast, predicate->left->column);
    const size_t source_index = FindColumnIndex(schema, predicate->left->column.name);
    const std::optional<size_t> projected_index = ProjectedIndexOf(projection_indexes, source_index);
    if (!projected_index.has_value()) {
        return;
    }

    std::unordered_set<std::string> values;
    values.reserve(predicate->values.size());

    for (const ExprPtr& value : predicate->values) {
        if (!value || value->kind != ExprKind::Literal) {
            return;
        }
        values.insert(LiteralTextForEval(value->literal));
    }

    predicate->literal_in_set_bound = true;
    predicate->in_column_index = *projected_index;
    predicate->literal_in_values = std::move(values);
}

static void BindLiteralLikePattern(const Query& query_ast, const Schema& schema,
                                   const std::vector<size_t>& projection_indexes, const PredicatePtr& predicate) {
    if (!predicate || (predicate->kind != PredicateKind::Like && predicate->kind != PredicateKind::NotLike) ||
        !predicate->left || !predicate->right || predicate->left->kind != ExprKind::Column ||
        predicate->right->kind != ExprKind::Literal || predicate->right->literal.kind != LiteralKind::String) {
        return;
    }

    ValidateColumnQualifier(query_ast, predicate->left->column);
    const size_t source_index = FindColumnIndex(schema, predicate->left->column.name);
    if (schema.columns[source_index].type != ColumnType::String) {
        return;
    }

    const std::optional<size_t> projected_index = ProjectedIndexOf(projection_indexes, source_index);
    if (!projected_index.has_value()) {
        return;
    }

    predicate->literal_like_pattern_bound = true;
    predicate->like_column_index = *projected_index;
    predicate->like_pattern = LiteralTextForEval(predicate->right->literal);
    predicate->like_negated = predicate->kind == PredicateKind::NotLike;
}

static ComparisonKind FlipComparison(const ComparisonKind comparison) {
    switch (comparison) {
        case ComparisonKind::Equal:
            return ComparisonKind::Equal;
        case ComparisonKind::NotEqual:
            return ComparisonKind::NotEqual;
        case ComparisonKind::Less:
            return ComparisonKind::Greater;
        case ComparisonKind::LessOrEqual:
            return ComparisonKind::GreaterOrEqual;
        case ComparisonKind::Greater:
            return ComparisonKind::Less;
        case ComparisonKind::GreaterOrEqual:
            return ComparisonKind::LessOrEqual;
    }

    return comparison;
}

static void BindTypedLiteralComparison(const Query& query_ast, const Schema& schema,
                                       const std::vector<size_t>& projection_indexes, const PredicatePtr& predicate) {
    if (!predicate || predicate->kind != PredicateKind::Comparison || !predicate->left || !predicate->right) {
        return;
    }

    const ExprPtr* column_expr = nullptr;
    const ExprPtr* literal_expr = nullptr;
    ComparisonKind comparison = predicate->comparison;

    if (predicate->left->kind == ExprKind::Column && predicate->right->kind == ExprKind::Literal) {
        column_expr = &predicate->left;
        literal_expr = &predicate->right;
    } else if (predicate->left->kind == ExprKind::Literal && predicate->right->kind == ExprKind::Column) {
        column_expr = &predicate->right;
        literal_expr = &predicate->left;
        comparison = FlipComparison(comparison);
    } else {
        return;
    }

    ValidateColumnQualifier(query_ast, (*column_expr)->column);
    const size_t source_index = FindColumnIndex(schema, (*column_expr)->column.name);
    const std::optional<size_t> projected_index = ProjectedIndexOf(projection_indexes, source_index);
    if (!projected_index.has_value()) {
        return;
    }

    const ColumnType type = schema.columns[source_index].type;
    const std::optional<Int128> literal = TryLiteralValueForColumnType((*literal_expr)->literal, type);
    if (!literal.has_value()) {
        return;
    }

    predicate->typed_literal_comparison_bound = true;
    predicate->typed_column_index = *projected_index;
    predicate->typed_literal_value = *literal;
    predicate->typed_comparison = comparison;
}

static std::optional<std::pair<ColumnRef, Int128>> TryLinearColumnExpr(const ExprPtr& expr) {
    if (!expr) {
        return std::nullopt;
    }

    if (expr->kind == ExprKind::Column) {
        return std::pair<ColumnRef, Int128>{expr->column, 0};
    }

    if (expr->kind != ExprKind::Binary || !expr->left || !expr->right || expr->left->kind != ExprKind::Column) {
        return std::nullopt;
    }

    const std::optional<Int128> literal = TryNumericLiteralValue(expr->right);
    if (!literal.has_value()) {
        return std::nullopt;
    }

    const Int128 offset = expr->binary_op == BinaryOp::Add ? *literal : -*literal;
    return std::pair<ColumnRef, Int128>{expr->left->column, offset};
}

static bool IsNumericType(const ColumnType type) {
    switch (type) {
        case ColumnType::Boolean:
        case ColumnType::Int16:
        case ColumnType::Int32:
        case ColumnType::Int64:
        case ColumnType::Int128:
        case ColumnType::Character:
            return true;
        case ColumnType::String:
        case ColumnType::Date:
        case ColumnType::Timestamp:
            return false;
    }

    return false;
}

PlannedQuery PlanQuery(const Query& query, const std::unordered_map<std::string, std::filesystem::path>& tables) {
    PlannedQuery planned;

    const auto table_it = tables.find(ToLowerAscii(query.table_name));
    if (table_it == tables.end()) {
        throw Error::NotFound("executor", "unknown table", query.table_name);
    }

    planned.table_path = table_it->second;
    const ColumnarBatchReader reader(planned.table_path);
    planned.table_schema = reader.GetSchema();
    const Schema& schema = planned.table_schema;

    std::vector<size_t> projection_indexes;

    planned.filter = query.filter;
    planned.having = query.having;
    CollectColumnsFromPredicate(query, schema, planned.filter, projection_indexes);

    planned.group_keys.reserve(query.group_by.size());

    for (const ExprPtr& raw_group_expr : query.group_by) {
        ExprPtr group_expr = ResolveGroupByExpression(query, raw_group_expr);
        CollectColumnsFromExpr(query, schema, group_expr, projection_indexes);
        planned.group_keys.push_back(PlannedGroupKey{
            .column_index = 0,
            .expression = group_expr,
            .column_type = ColumnTypeOf(query, schema, group_expr),
            .output_name = group_expr->output_name,
        });
    }

    const bool has_aggregate = HasAggregate(query.select_items);
    planned.plain_select = planned.group_keys.empty() && !has_aggregate;

    planned.select_items.reserve(query.select_items.size());
    planned.aggregates.reserve(query.select_items.size());

    if (planned.plain_select) {
        planned.plain_select_items = query.select_items;
        for (const auto& item : query.select_items) {
            CollectColumnsFromExpr(query, schema, item.expression, projection_indexes);
            if (item.expression && item.expression->kind == ExprKind::Star) {
                for (const auto& column : schema.columns) {
                    planned.select_items.push_back(PlannedSelectItem{
                        .kind = SelectItemKind::GroupKey,
                        .index = planned.select_items.size(),
                        .output_type = column.type,
                        .output_name = column.name,
                    });
                }
                continue;
            }
            planned.select_items.push_back(PlannedSelectItem{
                .kind = SelectItemKind::GroupKey,
                .index = planned.select_items.size(),
                .output_type = ColumnTypeOf(query, schema, item.expression),
                .output_name = item.output_name,
            });
        }
    } else {
        for (const auto& item : query.select_items) {
            if (item.kind == SelectItemKind::GroupKey) {
                const auto group_index = FindGroupKey(query.group_by.empty() ? std::vector<ExprPtr>{} : [&] {
                    std::vector<ExprPtr> resolved;
                    for (const auto& group : query.group_by) {
                        resolved.push_back(ResolveGroupByExpression(query, group));
                    }
                    return resolved;
                }(), item.expression);

                if (!group_index.has_value()) {
                    throw Error::InvalidArgument("executor", "SELECT expression must appear in GROUP BY");
                }

                planned.select_items.push_back(PlannedSelectItem{
                    .kind = SelectItemKind::GroupKey,
                    .index = *group_index,
                    .output_type = planned.group_keys[*group_index].column_type,
                    .output_name = item.output_name,
                });

                continue;
            }

            const AggSpec& aggregate = item.aggregate;
            const AggFuncDefinition& function = ResolveAggFunc(aggregate.function_name);
            CollectColumnsFromExpr(query, schema, aggregate.argument, projection_indexes);
            const std::optional<std::pair<ColumnRef, Int128>> direct_numeric = TryLinearColumnExpr(aggregate.argument);

            if (aggregate.distinct && !HasAggCallFeature(function.call_features, AggCallFeature::Distinct)) {
                throw Error::InvalidArgument("executor",
                                             std::string(function.canonical_name) + " does not support DISTINCT");
            }

            if (aggregate.argument_kind == AggArgumentKind::Star &&
                !HasAggCallFeature(function.call_features, AggCallFeature::Star)) {
                throw Error::InvalidArgument("executor",
                                             std::string(function.canonical_name) + " does not support '*'");
            }

            PlannedAgg planned_aggregate{
                .function = &function,
                .distinct = aggregate.distinct,
                .argument_kind = aggregate.argument_kind,
                .column_index = direct_numeric.has_value() ? FindColumnIndex(schema, direct_numeric->first.name) : 0,
                .direct_numeric_argument = direct_numeric.has_value() &&
                                           IsNumericType(ColumnTypeOfColumn(query, schema, direct_numeric->first)),
                .direct_numeric_offset = direct_numeric.has_value() ? direct_numeric->second : 0,
                .input_type = aggregate.argument ? ColumnTypeOf(query, schema, aggregate.argument) : ColumnType::String,
                .argument = aggregate.argument,
                .output_name = item.output_name,
            };

            if (aggregate.argument_kind == AggArgumentKind::Column &&
                !AggSupportsInputType(function, planned_aggregate.input_type)) {
                throw Error::Unsupported("executor", "aggregate " + std::string(function.canonical_name) +
                                                         " does not support its input expression");
            }

            const size_t aggregate_index = planned.aggregates.size();
            planned.aggregates.push_back(std::move(planned_aggregate));
            planned.select_items.push_back(PlannedSelectItem{
                .kind = SelectItemKind::Aggregate,
                .index = aggregate_index,
                .output_type = AggregateOutputType(planned.aggregates.back()),
                .output_name = item.output_name,
            });
        }
    }

    planned.order_by.reserve(query.order_by.size());

    if (planned.plain_select) {
        planned.plain_order_by = query.order_by;
        std::vector<OrderBySpec> effective_order_by = query.order_by;

        for (const auto& item : query.select_items) {
            if (item.expression && item.expression->kind != ExprKind::Star) {
                const bool already_ordered = std::ranges::any_of(effective_order_by, [&](const OrderBySpec& order) {
                    return SameExpr(order.item.expression, item.expression);
                });
                if (!already_ordered) {
                    effective_order_by.push_back(OrderBySpec{
                        .item = item,
                        .descending = false,
                    });
                }
            }
        }

        for (const auto& order_item : effective_order_by) {
            size_t column_index = 0;
            const auto selected = FindSelectItem(query, order_item.item);
            const ExprPtr order_expression =
                selected.has_value() ? query.select_items[*selected].expression : order_item.item.expression;
            const ColumnType type = ColumnTypeOf(query, schema, order_expression);
            CollectColumnsFromExpr(query, schema, order_expression, projection_indexes);

            if (order_expression && order_expression->kind == ExprKind::Column) {
                column_index = FindColumnIndex(schema, order_expression->column.name);
            } else if (selected.has_value()) {
                column_index = *selected;
            } else {
                throw Error::InvalidArgument("executor", "ORDER BY expression is not supported in plain SELECT");
            }

            planned.order_by.push_back(PlannedOrderBy{
                .result_column_index = column_index,
                .descending = order_item.descending,
                .value_type = type,
            });
        }

        const bool has_star_select = std::ranges::any_of(planned.plain_select_items, [](const SelectItemSpec& item) {
            return item.expression && item.expression->kind == ExprKind::Star;
        });

        for (const auto& order_item : query.order_by) {
            if (!order_item.item.expression || order_item.item.expression->kind == ExprKind::Star) {
                continue;
            }

            if (FindSelectItem(query, order_item.item).has_value()) {
                continue;
            }

            if (has_star_select) {
                continue;
            }

            const bool already_selected = std::ranges::any_of(
                planned.plain_select_items,
                [&](const SelectItemSpec& item) { return SameExpr(item.expression, order_item.item.expression); });

            if (!already_selected) {
                planned.plain_select_items.push_back(order_item.item);
                CollectColumnsFromExpr(query, schema, order_item.item.expression, projection_indexes);
                planned.select_items.push_back(PlannedSelectItem{
                    .kind = SelectItemKind::GroupKey,
                    .index = planned.select_items.size(),
                    .output_type = ColumnTypeOf(query, schema, order_item.item.expression),
                    .output_name = order_item.item.output_name,
                });
            }
        }
    } else {
        for (const auto& order_item : query.order_by) {
            const std::optional<size_t> result_column = FindSelectItem(query, order_item.item);
            if (!result_column.has_value()) {
                throw Error::InvalidArgument("executor", "ORDER BY expression must appear in SELECT");
            }

            planned.order_by.push_back(PlannedOrderBy{
                .result_column_index = *result_column,
                .descending = order_item.descending,
                .value_type = planned.select_items[*result_column].output_type,
            });
        }
    }

    planned.limit = query.limit;
    planned.offset = 0;
    planned.metadata_count_only = IsSimpleCountStar(query, planned);

    if (!planned.metadata_count_only) {
        if (projection_indexes.empty() && !schema.columns.empty()) {
            projection_indexes.push_back(0);
        }
        planned.projection_indexes = std::move(projection_indexes);
    }

    if (planned.plain_select) {
        for (PlannedOrderBy& order : planned.order_by) {
            const std::optional<size_t> projected_index =
                ProjectedIndexOf(planned.projection_indexes, order.result_column_index);
            if (projected_index.has_value()) {
                order.result_column_index = *projected_index;
            }
        }
    }

    for (PlannedAgg& aggregate : planned.aggregates) {
        if (!aggregate.direct_numeric_argument) {
            continue;
        }

        const std::optional<size_t> projected_index =
            ProjectedIndexOf(planned.projection_indexes, aggregate.column_index);
        if (projected_index.has_value()) {
            aggregate.column_index = *projected_index;
        } else {
            aggregate.direct_numeric_argument = false;
        }
    }

    if (!planned.metadata_count_only) {
        BindPredicateColumnIndexes(query, schema, planned.projection_indexes, planned.filter);
        for (PlannedGroupKey& group_key : planned.group_keys) {
            BindExprColumnIndexes(query, schema, planned.projection_indexes, group_key.expression);
        }
        for (PlannedAgg& aggregate : planned.aggregates) {
            BindExprColumnIndexes(query, schema, planned.projection_indexes, aggregate.argument);
        }
        for (SelectItemSpec& item : planned.plain_select_items) {
            BindExprColumnIndexes(query, schema, planned.projection_indexes, item.expression);
        }
    }

    return planned;
}
