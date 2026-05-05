#include <algorithm>

#include "executor/executor.h"
#include "executor/operator_utils.h"
#include "executor/planner_utils.h"
#include "io/columnar_batch_io.h"
#include "support/ascii.h"

PlannedQuery PlanQuery(const ParsedQuery& parsed,
                       const std::unordered_map<std::string, std::filesystem::path>& tables) {
    PlannedQuery planned;

    const auto table_it = tables.find(ToLowerAscii(parsed.table_name));
    if (table_it == tables.end()) {
        throw Error::NotFound("query_planner", "unknown table", parsed.table_name);
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

    if (parsed.filter.has_value()) {
        ValidateColumnQualifier(parsed, parsed.filter->column);
        const size_t source_index = FindColumnIndex(schema, parsed.filter->column.name);
        const ColumnType column_type = schema.columns[source_index].type;

        planned.filter = PlannedFilter{
            .column_index = register_projection(source_index),
            .column_type = column_type,
            .comparison = parsed.filter->comparison,
            .literal_text = NormalizeLiteral(parsed.filter->literal, column_type),
        };
    }

    planned.group_keys.reserve(parsed.group_by.size());
    for (const ColumnRef& group_column : parsed.group_by) {
        ValidateColumnQualifier(parsed, group_column);
        const size_t source_index = FindColumnIndex(schema, group_column.name);
        planned.group_keys.push_back(PlannedGroupKey{
            .column_index = register_projection(source_index),
            .column_type = schema.columns[source_index].type,
            .output_name = schema.columns[source_index].name,
        });
    }

    bool has_aggregate = false;
    for (const auto& item : parsed.select_items) {
        if (item.kind == SelectItemKind::Aggregate) {
            has_aggregate = true;
            break;
        }
    }

    planned.select_items.reserve(parsed.select_items.size());
    planned.aggregates.reserve(parsed.select_items.size());

    for (const auto& item : parsed.select_items) {
        if (item.kind == SelectItemKind::GroupKey) {
            if (parsed.group_by.empty()) {
                throw Error::Unsupported("query_planner", "plain column SELECT items require GROUP BY");
            }

            ValidateColumnQualifier(parsed, item.column);
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
        PlannedAgg planned_aggregate{
            .function = aggregate.function,
            .distinct = aggregate.distinct,
            .star = aggregate.star,
            .column_index = std::nullopt,
            .input_type = ColumnType::String,
            .output_name = item.output_name,
        };

        if (!aggregate.star) {
            ValidateColumnQualifier(parsed, aggregate.column);
            const size_t source_index = FindColumnIndex(schema, aggregate.column.name);
            planned_aggregate.column_index = register_projection(source_index);
            planned_aggregate.input_type = schema.columns[source_index].type;
            if (!AggSupportsInputType(*aggregate.function, planned_aggregate.input_type)) {
                throw Error::Unsupported("query_planner",
                                         "aggregate " + std::string(aggregate.function->canonical_name) +
                                             " does not support column '" + aggregate.column.name + "'");
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

    if (parsed.group_by.empty() && !has_aggregate) {
        throw Error::Unsupported("query_planner", "plain SELECT without aggregates is not supported");
    }

    planned.order_by.reserve(parsed.order_by.size());
    for (const auto& order_item : parsed.order_by) {
        const auto it = std::ranges::find_if(parsed.select_items, [&](const SelectItemSpec& select_item) {
            return SameSelectItem(select_item, order_item.item) || SameOutputName(select_item, order_item.item);
        });
        if (it == parsed.select_items.end()) {
            throw Error::InvalidArgument("query_planner", "ORDER BY expression must appear in SELECT");
        }

        const size_t result_column_index = static_cast<size_t>(std::distance(parsed.select_items.begin(), it));
        planned.order_by.push_back(PlannedOrderBy{
            .result_column_index = result_column_index,
            .descending = order_item.descending,
            .value_type = planned.select_items[result_column_index].output_type,
        });
    }

    planned.limit = parsed.limit;

    if (planned.projection_indexes.empty() && !schema.columns.empty()) {
        planned.projection_indexes.push_back(0);
    }

    return planned;
}
