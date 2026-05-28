#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "executor/operator.h"

std::unique_ptr<Operator> CreateMetadataCountOperator(std::filesystem::path path, std::string output_name);
std::unique_ptr<Operator> CreateMetadataExtremaOperator(std::filesystem::path path, std::vector<PlannedAgg> aggregates);
std::unique_ptr<Operator> CreateScanOperator(std::filesystem::path path, std::vector<size_t> projection_indexes,
                                             PredicatePtr filter);
std::unique_ptr<Operator> CreateFilterOperator(std::unique_ptr<Operator> child, PredicatePtr filter);
std::unique_ptr<Operator> CreateEnsureSchemaOperator(std::unique_ptr<Operator> child, Schema schema);
std::unique_ptr<Operator> CreateProjectionOperator(std::unique_ptr<Operator> child, std::vector<SelectItemSpec> items,
                                                   Schema source_schema);
std::unique_ptr<Operator> CreateAggOperator(std::unique_ptr<Operator> child, std::vector<PlannedAgg> aggregates,
                                            PredicatePtr filter);
std::unique_ptr<Operator> CreateGroupAggOperator(std::unique_ptr<Operator> child,
                                                 std::vector<PlannedGroupKey> group_keys,
                                                 const std::vector<PlannedAgg>& aggregates,
                                                 std::vector<PlannedSelectItem> select_items, PredicatePtr filter,
                                                 PredicatePtr having, bool sort_by_group_keys);
std::unique_ptr<Operator> CreateGroupAggTopKOperator(std::unique_ptr<Operator> child,
                                                     std::vector<PlannedGroupKey> group_keys,
                                                     const std::vector<PlannedAgg>& aggregates,
                                                     std::vector<PlannedSelectItem> select_items, PredicatePtr filter,
                                                     std::vector<PlannedOrderBy> order_by, size_t limit);

void ApplyOrderOffsetLimit(std::unique_ptr<Operator>& root, const PlannedQuery& planned, bool& limit_applied_by_top_k);
Schema ProjectionOutputSchema(const std::vector<SelectItemSpec>& items, const Schema& source_schema);

std::string NormalizeLiteralForEval(const QueryLiteral& literal);
std::optional<size_t> TryFindBatchColumn(const Schema& schema, std::string_view name);
std::string EvalExpr(const ExprPtr& expr, const Batch& batch, size_t row);
bool EvaluatePredicate(const PredicatePtr& predicate, const Batch& batch, size_t row);
void SelectRowsMatchingPredicate(const PredicatePtr& predicate, const Batch& batch, std::vector<size_t>& rows);
Schema BuildSelectOutputSchema(const std::vector<PlannedSelectItem>& select_items);
