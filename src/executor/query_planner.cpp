#include "executor/query_planner.h"

#include <algorithm>

#include "common/ascii.h"
#include "common/error.h"
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

PlannedQuery PlanQuery(const Query& query_ast, const std::unordered_map<std::string, std::filesystem::path>& tables) {
    PlannedQuery planned;

    const auto table_it = tables.find(ToLowerAscii(query_ast.table_name));
    if (table_it == tables.end()) {
        throw Error::NotFound("executor", "unknown table", query_ast.table_name);
    }

    planned.table_path = table_it->second;
    const ColumnarBatchReader reader(planned.table_path);
    planned.table_schema = reader.GetSchema();
    const Schema& schema = planned.table_schema;

    planned.projection_indexes.reserve(schema.columns.size());
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        planned.projection_indexes.push_back(i);
    }

    planned.filter = query_ast.filter;
    planned.having = query_ast.having;

    planned.group_keys.reserve(query_ast.group_by.size());

    for (const ExprPtr& raw_group_expr : query_ast.group_by) {
        ExprPtr group_expr = ResolveGroupByExpression(query_ast, raw_group_expr);
        planned.group_keys.push_back(PlannedGroupKey{
            .column_index = 0,
            .expression = group_expr,
            .column_type = ColumnTypeOf(query_ast, schema, group_expr),
            .output_name = group_expr->output_name,
        });
    }

    const bool has_aggregate = HasAggregate(query_ast.select_items);
    planned.plain_select = planned.group_keys.empty() && !has_aggregate;

    planned.select_items.reserve(query_ast.select_items.size());
    planned.aggregates.reserve(query_ast.select_items.size());

    if (planned.plain_select) {
        planned.plain_select_items = query_ast.select_items;
        for (const auto& item : query_ast.select_items) {
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
                .output_type = ColumnTypeOf(query_ast, schema, item.expression),
                .output_name = item.output_name,
            });
        }
    } else {
        for (const auto& item : query_ast.select_items) {
            if (item.kind == SelectItemKind::GroupKey) {
                const auto group_index = FindGroupKey(query_ast.group_by.empty() ? std::vector<ExprPtr>{} : [&] {
                    std::vector<ExprPtr> resolved;
                    for (const auto& group : query_ast.group_by) {
                        resolved.push_back(ResolveGroupByExpression(query_ast, group));
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
                .column_index = 0,
                .input_type =
                    aggregate.argument ? ColumnTypeOf(query_ast, schema, aggregate.argument) : ColumnType::String,
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

    planned.order_by.reserve(query_ast.order_by.size());

    if (planned.plain_select) {
        planned.plain_order_by = query_ast.order_by;
        std::vector<OrderBySpec> effective_order_by = query_ast.order_by;

        for (const auto& item : query_ast.select_items) {
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
            const ColumnType type = ColumnTypeOf(query_ast, schema, order_item.item.expression);

            if (order_item.item.expression && order_item.item.expression->kind == ExprKind::Column) {
                column_index = FindColumnIndex(schema, order_item.item.expression->column.name);
            } else if (const auto selected = FindSelectItem(query_ast, order_item.item); selected.has_value()) {
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

        for (const auto& order_item : query_ast.order_by) {
            if (!order_item.item.expression || order_item.item.expression->kind == ExprKind::Star) {
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
                planned.select_items.push_back(PlannedSelectItem{
                    .kind = SelectItemKind::GroupKey,
                    .index = planned.select_items.size(),
                    .output_type = ColumnTypeOf(query_ast, schema, order_item.item.expression),
                    .output_name = order_item.item.output_name,
                });
            }
        }
    } else {
        for (const auto& order_item : query_ast.order_by) {
            const std::optional<size_t> result_column = FindSelectItem(query_ast, order_item.item);
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

    planned.limit = query_ast.limit;
    planned.offset = 0;

    return planned;
}
