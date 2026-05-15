#include <algorithm>
#include <unordered_map>
#include <utility>
#include <vector>

#include "executor/aggregate_state.h"
#include "executor/operator.h"
#include "executor/operator_utils.h"
#include "io/columnar_batch_io.h"
#include "io/fileio.h"
#include "support/error.h"

class ScanOperator final : public Operator {
   public:
    ScanOperator(std::filesystem::path path, std::vector<size_t> projection_indexes)
        : path_(std::move(path)), input_(path_), metadata_(ColumnarBatchReader(path_).GetMetadata()) {
        if (metadata_.schema.columns.empty()) {
            throw Error::InvalidData("operators", "columnar schema is empty", path_.string());
        }

        projection_indexes_ = std::move(projection_indexes);
        projected_schema_.columns.reserve(projection_indexes_.size());

        for (const size_t source_index : projection_indexes_) {
            if (source_index >= metadata_.schema.columns.size()) {
                throw Error::OutOfRange("operators", "projection index out of range", path_.string());
            }
            projected_schema_.columns.push_back(metadata_.schema.columns[source_index]);
        }
    }

   public:
    std::optional<Batch> Next() override {
        if (next_group_ >= metadata_.row_groups.size()) {
            return std::nullopt;
        }

        const auto& row_group = metadata_.row_groups[next_group_++];
        Batch batch(projected_schema_, row_group.row_count);

        for (size_t projected_index = 0; projected_index < projection_indexes_.size(); ++projected_index) {
            const size_t source_index = projection_indexes_[projected_index];
            const auto& chunk = row_group.columns[source_index];
            batch.ColumnAt(projected_index).ReadFrom(input_.StreamAt(chunk.offset), row_group.row_count, chunk.size);
        }

        return batch;
    }

   private:
    std::filesystem::path path_;
    InputFile input_;

    ColumnarMetadata metadata_;
    std::vector<size_t> projection_indexes_;
    Schema projected_schema_;
    size_t next_group_ = 0;
};

class FilterOperator final : public Operator {
   public:
    FilterOperator(std::unique_ptr<Operator> child, PlannedFilter filter)
        : child_(std::move(child)), filter_(std::move(filter)) {}

   public:
    std::optional<Batch> Next() override {
        while (auto batch = child_->Next()) {
            Batch filtered(batch->GetSchema());

            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                const std::string left = MaterializeValue(filter_.left, *batch, row);
                const std::string right = MaterializeValue(filter_.right, *batch, row);

                if (!MatchesComparison(filter_.comparison_type, left, right, filter_.comparison)) {
                    continue;
                }

                for (size_t column_index = 0; column_index < batch->ColumnsCount(); ++column_index) {
                    filtered.ColumnAt(column_index).AppendFromString(batch->ColumnAt(column_index).ValueAsString(row));
                }
            }

            if (filtered.RowsCount() > 0) {
                return filtered;
            }
        }

        return std::nullopt;
    }

   private:
    static std::string MaterializeValue(const PlannedValue& value, const Batch& batch, const size_t row) {
        if (value.kind == PlannedValueKind::Literal) {
            return value.literal_text;
        }
        return batch.ColumnAt(value.column_index).ValueAsString(row);
    }

   private:
    std::unique_ptr<Operator> child_;
    PlannedFilter filter_;
};

struct AggBinding {
    PlannedAgg aggregate;
    std::unique_ptr<AggState> state;
};

class AggOperator final : public Operator {
   public:
    AggOperator(std::unique_ptr<Operator> child, const std::vector<PlannedAgg>& aggregates) : child_(std::move(child)) {
        bindings_.reserve(aggregates.size());
        for (const auto& aggregate : aggregates) {
            bindings_.push_back(AggBinding{
                .aggregate = aggregate,
                .state = CreateAggState(aggregate),
            });
        }
    }

   public:
    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }
        returned_ = true;

        while (auto batch = child_->Next()) {
            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                for (const auto& binding : bindings_) {
                    if (binding.aggregate.argument_kind == AggArgumentKind::Column) {
                        std::string materialized = batch->ColumnAt(binding.aggregate.column_index).ValueAsString(row);
                        binding.state->ConsumeValue(materialized);
                    } else {
                        binding.state->ConsumeRow();
                    }
                }
            }
        }

        Schema schema;
        schema.columns.reserve(bindings_.size());

        for (const auto& binding : bindings_) {
            schema.columns.push_back(ColumnSchema{
                .name = binding.aggregate.output_name,
                .type = AggregateOutputType(binding.aggregate),
            });
        }

        Batch result(schema);

        for (size_t i = 0; i < bindings_.size(); ++i) {
            result.ColumnAt(i).AppendFromString(bindings_[i].state->Finalize());
        }

        return result;
    }

   private:
    std::unique_ptr<Operator> child_;
    std::vector<AggBinding> bindings_;
    bool returned_ = false;
};

class GroupAggOperator final : public Operator {
   public:
    GroupAggOperator(std::unique_ptr<Operator> child, std::vector<PlannedGroupKey> group_keys,
                     std::vector<PlannedAgg> aggregates, std::vector<PlannedSelectItem> select_items,
                     std::vector<PlannedOrderBy> order_by)
        : child_(std::move(child)),
          group_keys_(std::move(group_keys)),
          aggregates_(std::move(aggregates)),
          select_items_(std::move(select_items)),
          order_by_(std::move(order_by)) {}

   public:
    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }
        returned_ = true;

        while (auto batch = child_->Next()) {
            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                std::vector<std::string> key_values;
                key_values.reserve(group_keys_.size());

                std::string group_id;
                for (const auto& group_key : group_keys_) {
                    std::string value = batch->ColumnAt(group_key.column_index).ValueAsString(row);
                    group_id += std::to_string(value.size());
                    group_id.push_back(':');
                    group_id += value;
                    group_id.push_back('|');
                    key_values.push_back(std::move(value));
                }

                size_t group_index = 0;
                const auto it = group_indexes_.find(group_id);
                if (it == group_indexes_.end()) {
                    group_index = groups_.size();
                    group_indexes_.emplace(std::move(group_id), group_index);
                    groups_.push_back(GroupState{
                        .key_values = std::move(key_values),
                        .states = CreateStates(),
                    });
                } else {
                    group_index = it->second;
                }

                GroupState& group = groups_[group_index];
                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    const PlannedAgg& aggregate = aggregates_[i];
                    if (aggregate.argument_kind == AggArgumentKind::Column) {
                        std::string materialized = batch->ColumnAt(aggregate.column_index).ValueAsString(row);
                        group.states[i]->ConsumeValue(materialized);
                    } else {
                        group.states[i]->ConsumeRow();
                    }
                }
            }
        }

        Schema schema;
        schema.columns.reserve(select_items_.size());
        for (const auto& item : select_items_) {
            schema.columns.push_back(ColumnSchema{
                .name = item.output_name,
                .type = item.output_type,
            });
        }

        std::vector<std::vector<std::string>> rows;
        rows.reserve(groups_.size());
        for (const auto& group : groups_) {
            rows.push_back(MaterializeRow(group));
        }

        if (!order_by_.empty()) {
            std::ranges::stable_sort(
                rows, [&](const std::vector<std::string>& lhs, const std::vector<std::string>& rhs) {
                    for (const auto& order : order_by_) {
                        const int comparison = CompareValues(order.value_type, lhs[order.result_column_index],
                                                             rhs[order.result_column_index]);
                        if (comparison != 0) {
                            return order.descending ? comparison > 0 : comparison < 0;
                        }
                    }

                    for (size_t i = 0; i < select_items_.size(); ++i) {
                        if (select_items_[i].kind != SelectItemKind::GroupKey) {
                            continue;
                        }
                        const int comparison = CompareValues(select_items_[i].output_type, lhs[i], rhs[i]);
                        if (comparison != 0) {
                            return comparison > 0;
                        }
                    }
                    return false;
                });
        }

        Batch result(schema, rows.size());
        for (const auto& row : rows) {
            for (size_t i = 0; i < row.size(); ++i) {
                result.ColumnAt(i).AppendFromString(row[i]);
            }
        }

        return result;
    }

   private:
    struct GroupState {
        std::vector<std::string> key_values;
        std::vector<std::unique_ptr<AggState>> states;
    };

    std::vector<std::unique_ptr<AggState>> CreateStates() const {
        std::vector<std::unique_ptr<AggState>> states;
        states.reserve(aggregates_.size());
        for (const auto& aggregate : aggregates_) {
            states.push_back(CreateAggState(aggregate));
        }
        return states;
    }

    std::vector<std::string> MaterializeRow(const GroupState& group) const {
        std::vector<std::string> row;
        row.reserve(select_items_.size());

        for (const auto& item : select_items_) {
            if (item.kind == SelectItemKind::GroupKey) {
                row.push_back(group.key_values[item.index]);
            } else {
                row.push_back(group.states[item.index]->Finalize());
            }
        }

        return row;
    }

   private:
    std::unique_ptr<Operator> child_;
    std::vector<PlannedGroupKey> group_keys_;
    std::vector<PlannedAgg> aggregates_;
    std::vector<PlannedSelectItem> select_items_;
    std::vector<PlannedOrderBy> order_by_;
    std::unordered_map<std::string, size_t> group_indexes_;
    std::vector<GroupState> groups_;
    bool returned_ = false;
};

class LimitOperator final : public Operator {
   public:
    LimitOperator(std::unique_ptr<Operator> child, const size_t limit) : child_(std::move(child)), remaining_(limit) {}

   public:
    std::optional<Batch> Next() override {
        if (remaining_ == 0) {
            return std::nullopt;
        }

        while (auto batch = child_->Next()) {
            if (batch->RowsCount() <= remaining_) {
                remaining_ -= batch->RowsCount();
                return batch;
            }

            Batch limited(batch->GetSchema(), remaining_);
            for (size_t row = 0; row < remaining_; ++row) {
                for (size_t column_index = 0; column_index < batch->ColumnsCount(); ++column_index) {
                    limited.ColumnAt(column_index).AppendFromString(batch->ColumnAt(column_index).ValueAsString(row));
                }
            }
            remaining_ = 0;
            return limited;
        }

        remaining_ = 0;
        return std::nullopt;
    }

   private:
    std::unique_ptr<Operator> child_;
    size_t remaining_;
};

std::unique_ptr<Operator> BuildPlan(const PlannedQuery& planned) {
    std::unique_ptr<Operator> root = std::make_unique<ScanOperator>(planned.table_path, planned.projection_indexes);
    if (planned.filter.has_value()) {
        root = std::make_unique<FilterOperator>(std::move(root), *planned.filter);
    }
    if (!planned.group_keys.empty()) {
        root = std::make_unique<GroupAggOperator>(std::move(root), planned.group_keys, planned.aggregates,
                                                  planned.select_items, planned.order_by);
    } else {
        root = std::make_unique<AggOperator>(std::move(root), planned.aggregates);
    }
    if (planned.limit.has_value()) {
        root = std::make_unique<LimitOperator>(std::move(root), *planned.limit);
    }
    return root;
}
