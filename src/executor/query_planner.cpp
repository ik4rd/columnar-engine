#include "executor/query_planner.h"

#include <algorithm>

#include "executor/operator_utils.h"
#include "executor/planner_utils.h"
#include "io/columnar_batch_io.h"
#include "support/ascii.h"
#include "support/error.h"

static std::optional<size_t> TryFindColumnIndex(const Schema& schema, const std::string_view column_name) {
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (SameColumnName(schema.columns[i].name, column_name)) {
            return i;
        }
    }
    return std::nullopt;
}

PlannedQuery PlanQuery(const Query& query_ast, const std::unordered_map<std::string, std::filesystem::path>& tables) {
    PlannedQuery planned;

    const auto table_it = tables.find(ToLowerAscii(query_ast.table_name));
    if (table_it == tables.end()) {
        throw Error::NotFound("query_planner", "unknown table", query_ast.table_name);
    }

    planned.table_path = table_it->second;

    const ColumnarBatchReader reader(planned.table_path);
    const Schema& schema = reader.GetSchema();

    const auto register_projection = [&](const size_t source_index) -> size_t {
        const auto it = std::ranges::find(planned.projection_indexes, source_index);
        if (it != planned.projection_indexes.end()) {
            return static_cast<size_t>(std::distance(planned.projection_indexes.begin(), it));
        }
        planned.projection_indexes.push_back(source_index);
        return planned.projection_indexes.size() - 1;
    };

    struct PlannedFilterValue {
        PlannedValue value;
        std::optional<QueryLiteral> literal;
    };

    const auto plan_filter_value = [&](const QueryValue& value) -> PlannedFilterValue {
        if (value.kind == QueryValueKind::Column) {
            ValidateColumnQualifier(query_ast, value.column);
            const size_t source_index = FindColumnIndex(schema, value.column.name);
            return PlannedFilterValue{
                .value =
                    PlannedValue{
                        .kind = PlannedValueKind::Column,
                        .column_type = schema.columns[source_index].type,
                        .column_index = register_projection(source_index),
                        .literal_text = {},
                    },
                .literal = std::nullopt,
            };
        }

        if (value.literal.kind == LiteralKind::Identifier) {
            if (const std::optional<size_t> source_index = TryFindColumnIndex(schema, value.literal.text);
                source_index.has_value()) {
                return PlannedFilterValue{
                    .value =
                        PlannedValue{
                            .kind = PlannedValueKind::Column,
                            .column_type = schema.columns[*source_index].type,
                            .column_index = register_projection(*source_index),
                            .literal_text = {},
                        },
                    .literal = std::nullopt,
                };
            }
        }

        return PlannedFilterValue{
            .value =
                PlannedValue{
                    .kind = PlannedValueKind::Literal,
                    .column_type = ColumnType::String,
                    .column_index = 0,
                    .literal_text = {},
                },
            .literal = value.literal,
        };
    };

    if (query_ast.filter.has_value()) {
        PlannedFilterValue left = plan_filter_value(query_ast.filter->left);
        PlannedFilterValue right = plan_filter_value(query_ast.filter->right);

        const ColumnType comparison_type = left.value.kind == PlannedValueKind::Column    ? left.value.column_type
                                           : right.value.kind == PlannedValueKind::Column ? right.value.column_type
                                                                                          : ColumnType::String;
        if (left.literal.has_value()) {
            left.value.column_type = comparison_type;
            left.value.literal_text = NormalizeLiteral(*left.literal, comparison_type);
        }
        if (right.literal.has_value()) {
            right.value.column_type = comparison_type;
            right.value.literal_text = NormalizeLiteral(*right.literal, comparison_type);
        }

        planned.filter = PlannedFilter{
            .left = std::move(left.value),
            .comparison = query_ast.filter->comparison,
            .right = std::move(right.value),
            .comparison_type = comparison_type,
        };
    }

    planned.group_keys.reserve(query_ast.group_by.size());
    for (const ColumnRef& group_column : query_ast.group_by) {
        ValidateColumnQualifier(query_ast, group_column);
        const size_t source_index = FindColumnIndex(schema, group_column.name);
        planned.group_keys.push_back(PlannedGroupKey{
            .column_index = register_projection(source_index),
            .column_type = schema.columns[source_index].type,
            .output_name = schema.columns[source_index].name,
        });
    }

    bool has_aggregate = false;
    for (const auto& item : query_ast.select_items) {
        if (item.kind == SelectItemKind::Aggregate) {
            has_aggregate = true;
            break;
        }
    }

    planned.select_items.reserve(query_ast.select_items.size());
    planned.aggregates.reserve(query_ast.select_items.size());

    for (const auto& item : query_ast.select_items) {
        if (item.kind == SelectItemKind::GroupKey) {
            if (query_ast.group_by.empty()) {
                throw Error::Unsupported("query_planner", "plain column SELECT items require GROUP BY");
            }

            ValidateColumnQualifier(query_ast, item.column);
            const auto it = std::ranges::find_if(planned.group_keys, [&](const PlannedGroupKey& group_key) {
                return SameColumnName(group_key.output_name, item.column.name);
            });
            if (it == planned.group_keys.end()) {
                throw Error::InvalidArgument("query_planner",
                                             "column '" + item.column.name + "' must appear in GROUP BY");
            }

            planned.select_items.push_back(PlannedSelectItem{
                .kind = SelectItemKind::GroupKey,
                .index = static_cast<size_t>(std::distance(planned.group_keys.begin(), it)),
                .output_type = it->column_type,
                .output_name = item.output_name,
            });
            continue;
        }

        const AggSpec& aggregate = item.aggregate;
        const AggFuncDefinition& function = ResolveAggFunc(aggregate.function_name);

        if (aggregate.distinct && !function.distinct) {
            throw Error::InvalidArgument("query_planner",
                                         std::string(function.canonical_name) + " does not support DISTINCT");
        }
        if (aggregate.argument_kind == AggArgumentKind::Star && !function.star) {
            throw Error::InvalidArgument("query_planner",
                                         std::string(function.canonical_name) + " does not support '*'");
        }

        PlannedAgg planned_aggregate{
            .function = &function,
            .distinct = aggregate.distinct,
            .argument_kind = aggregate.argument_kind,
            .column_index = 0,
            .input_type = ColumnType::String,
            .output_name = item.output_name,
        };

        if (aggregate.argument_kind == AggArgumentKind::Column) {
            ValidateColumnQualifier(query_ast, aggregate.column);
            const size_t source_index = FindColumnIndex(schema, aggregate.column.name);
            planned_aggregate.column_index = register_projection(source_index);
            planned_aggregate.input_type = schema.columns[source_index].type;
            if (!AggSupportsInputType(function, planned_aggregate.input_type)) {
                throw Error::Unsupported("query_planner", "aggregate " + std::string(function.canonical_name) +
                                                              " does not support column '" + aggregate.column.name +
                                                              "'");
            }
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

    if (query_ast.group_by.empty() && !has_aggregate) {
        throw Error::Unsupported("query_planner", "plain SELECT without aggregates is not supported");
    }

    planned.order_by.reserve(query_ast.order_by.size());
    for (const auto& order_item : query_ast.order_by) {
        const auto it = std::ranges::find_if(query_ast.select_items, [&](const SelectItemSpec& select_item) {
            return SameSelectItem(select_item, order_item.item) || SameOutputName(select_item, order_item.item);
        });
        if (it == query_ast.select_items.end()) {
            throw Error::InvalidArgument("query_planner", "ORDER BY expression must appear in SELECT");
        }

        const size_t result_column_index = static_cast<size_t>(std::distance(query_ast.select_items.begin(), it));
        planned.order_by.push_back(PlannedOrderBy{
            .result_column_index = result_column_index,
            .descending = order_item.descending,
            .value_type = planned.select_items[result_column_index].output_type,
        });
    }

    planned.limit = query_ast.limit;

    if (planned.projection_indexes.empty() && !schema.columns.empty()) {
        planned.projection_indexes.push_back(0);
    }

    return planned;
}
