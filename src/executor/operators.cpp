#include "executor/operator.h"
#include "executor/operators_internal.h"

std::unique_ptr<Operator> BuildPlan(const PlannedQuery& planned) {
    if (planned.metadata_count_only) {
        return CreateMetadataCountOperator(planned.table_path, planned.aggregates.front().output_name);
    }

    if (planned.metadata_extrema_only) {
        return CreateMetadataExtremaOperator(planned.table_path, planned.aggregates);
    }

    std::unique_ptr<Operator> root = CreateScanOperator(planned.table_path, planned.projection_indexes, planned.filter);

    if (planned.filter && planned.plain_select) {
        root = CreateFilterOperator(std::move(root), planned.filter);
    }

    if (planned.plain_select) {
        const Schema output_schema = ProjectionOutputSchema(planned.plain_select_items, planned.table_schema);
        root = CreateProjectionOperator(std::move(root), planned.plain_select_items, planned.table_schema);
        root = CreateEnsureSchemaOperator(std::move(root), output_schema);

        bool limit_applied_by_top_k = false;
        ApplyOrderOffsetLimit(root, planned, limit_applied_by_top_k);

        return root;
    }

    bool limit_applied_by_top_k = false;

    if (!planned.group_keys.empty()) {
        root = CreateGroupAggOperator(std::move(root), planned.group_keys, planned.aggregates, planned.select_items,
                                      planned.filter, planned.having, planned.order_by.empty());
    } else {
        root = CreateAggOperator(std::move(root), planned.aggregates, planned.filter);
    }

    ApplyOrderOffsetLimit(root, planned, limit_applied_by_top_k);
    root = CreateEnsureSchemaOperator(std::move(root), BuildSelectOutputSchema(planned.select_items));

    return root;
}
