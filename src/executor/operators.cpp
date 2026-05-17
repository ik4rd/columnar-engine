#include <algorithm>
#include <compare>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/error.h"
#include "executor/aggregate_state.h"
#include "executor/operator.h"
#include "executor/operator_utils.h"
#include "io/columnar_batch.h"
#include "io/file.h"

class ScanOperator final : public Operator {
   public:
    ScanOperator(std::filesystem::path path, std::vector<size_t> projection_indexes)
        : path_(std::move(path)), input_(path_), metadata_(ColumnarBatchReader(path_).GetMetadata()) {
        if (metadata_.schema.columns.empty()) {
            throw Error::MalformedData("executor", "columnar schema is empty", path_.string());
        }

        projection_indexes_ = std::move(projection_indexes);
        projected_schema_.columns.reserve(projection_indexes_.size());

        for (const size_t source_index : projection_indexes_) {
            if (source_index >= metadata_.schema.columns.size()) {
                throw Error::OutOfRange("executor", "projection index out of range", path_.string());
            }

            projected_schema_.columns.push_back(metadata_.schema.columns[source_index]);
        }
    }

    std::optional<Batch> Next() override {
        if (next_group_ >= metadata_.row_groups.size()) {
            return std::nullopt;
        }

        const auto& row_group = metadata_.row_groups[next_group_++];
        Batch batch(projected_schema_, row_group.row_count);

        for (size_t projected_index = 0; projected_index < projection_indexes_.size(); ++projected_index) {
            const size_t source_index = projection_indexes_[projected_index];
            const auto& chunk = row_group.columns[source_index];

            batch.ReadColumnFrom(projected_index, input_.StreamAt(chunk.offset), row_group.row_count, chunk.size);
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
                    filtered.AppendValueFromString(column_index, batch->ColumnAt(column_index).ValueAsString(row));
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

    std::unique_ptr<Operator> child_;
    PlannedFilter filter_;
};

class AggInput {
   public:
    virtual ~AggInput() = default;

    virtual void Consume(const Batch& batch, size_t row, AggState& state) const = 0;
};

class ColumnAggInput final : public AggInput {
   public:
    explicit ColumnAggInput(const size_t column_index) : column_index_(column_index) {}

    void Consume(const Batch& batch, const size_t row, AggState& state) const override {
        const std::string materialized = batch.ColumnAt(column_index_).ValueAsString(row);
        state.ConsumeValue(materialized);
    }

   private:
    size_t column_index_ = 0;
};

class RowAggInput final : public AggInput {
   public:
    void Consume(const Batch&, const size_t, AggState& state) const override { state.ConsumeRow(); }
};

static std::unique_ptr<AggInput> CreateAggInput(const PlannedAgg& aggregate) {
    if (aggregate.argument_kind == AggArgumentKind::Column) {
        return std::make_unique<ColumnAggInput>(aggregate.column_index);
    }
    return std::make_unique<RowAggInput>();
}

struct AggBinding {
    PlannedAgg aggregate;
    std::unique_ptr<AggInput> input;
    std::unique_ptr<AggState> state;
};

struct MaterializedGroupKey {
    std::string id;
    std::vector<std::string> values;
};

class GroupKeyEncoder {
   public:
    explicit GroupKeyEncoder(std::vector<PlannedGroupKey> group_keys) : group_keys_(std::move(group_keys)) {}

    MaterializedGroupKey Encode(const Batch& batch, const size_t row) const {
        MaterializedGroupKey key;
        key.values.reserve(group_keys_.size());

        for (const auto& group_key : group_keys_) {
            std::string value = batch.ColumnAt(group_key.column_index).ValueAsString(row);
            AppendValue(key.id, value);
            key.values.push_back(std::move(value));
        }

        return key;
    }

   private:
    static void AppendValue(std::string& id, const std::string_view value) {
        id += std::to_string(value.size());
        id.push_back(':');

        id += value;
    }

    std::vector<PlannedGroupKey> group_keys_;
};

class AggOperator final : public Operator {
   public:
    AggOperator(std::unique_ptr<Operator> child, const std::vector<PlannedAgg>& aggregates) : child_(std::move(child)) {
        bindings_.reserve(aggregates.size());

        for (const auto& aggregate : aggregates) {
            bindings_.push_back(AggBinding{
                .aggregate = aggregate,
                .input = CreateAggInput(aggregate),
                .state = CreateAggState(aggregate),
            });
        }
    }

    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }

        returned_ = true;

        while (auto batch = child_->Next()) {
            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                for (const auto& binding : bindings_) {
                    binding.input->Consume(*batch, row, *binding.state);
                }
            }
        }

        Schema schema;
        schema.columns.reserve(bindings_.size());

        for (const auto& binding : bindings_) {
            schema.columns.push_back(
                ColumnSchema(binding.aggregate.output_name, AggregateOutputType(binding.aggregate)));
        }

        Batch result(schema);

        for (size_t i = 0; i < bindings_.size(); ++i) {
            result.AppendValueFromString(i, bindings_[i].state->Finalize());
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
                     const std::vector<PlannedAgg>& aggregates, std::vector<PlannedSelectItem> select_items,
                     const bool sort_by_group_keys)
        : child_(std::move(child)),
          group_key_encoder_(std::move(group_keys)),
          select_items_(std::move(select_items)),
          sort_by_group_keys_(sort_by_group_keys) {
        bindings_.reserve(aggregates.size());

        for (const auto& aggregate : aggregates) {
            bindings_.push_back(GroupAggBinding{
                .aggregate = aggregate,
                .input = CreateAggInput(aggregate),
            });
        }
    }

    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }

        returned_ = true;

        while (auto batch = child_->Next()) {
            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                MaterializedGroupKey key = group_key_encoder_.Encode(*batch, row);

                size_t group_index = 0;
                const auto it = group_indexes_.find(key.id);
                if (it == group_indexes_.end()) {
                    group_index = groups_.size();
                    group_indexes_.emplace(std::move(key.id), group_index);

                    groups_.push_back(GroupState{
                        .key_values = std::move(key.values),
                        .states = CreateStates(),
                    });
                } else {
                    group_index = it->second;
                }

                GroupState& group = groups_[group_index];
                for (size_t i = 0; i < bindings_.size(); ++i) {
                    bindings_[i].input->Consume(*batch, row, *group.states[i]);
                }
            }
        }

        Schema schema;
        schema.columns.reserve(select_items_.size());

        for (const auto& item : select_items_) {
            schema.columns.push_back(ColumnSchema(item.output_name, item.output_type));
        }

        std::vector<std::vector<std::string>> rows;
        rows.reserve(groups_.size());

        for (const auto& group : groups_) {
            rows.push_back(MaterializeRow(group));
        }

        if (sort_by_group_keys_) {
            SortRowsBySelectedGroupKeys(rows);
        }

        Batch result(schema, rows.size());

        for (const auto& row : rows) {
            for (size_t i = 0; i < row.size(); ++i) {
                result.AppendValueFromString(i, row[i]);
            }
        }

        return result;
    }

   private:
    struct GroupState {
        std::vector<std::string> key_values;
        std::vector<std::unique_ptr<AggState>> states;
    };

    struct GroupAggBinding {
        PlannedAgg aggregate;
        std::unique_ptr<AggInput> input;
    };

    std::vector<std::unique_ptr<AggState>> CreateStates() const {
        std::vector<std::unique_ptr<AggState>> states;
        states.reserve(bindings_.size());

        for (const auto& binding : bindings_) {
            states.push_back(CreateAggState(binding.aggregate));
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

    void SortRowsBySelectedGroupKeys(std::vector<std::vector<std::string>>& rows) const {
        std::vector<size_t> group_key_columns;

        for (size_t i = 0; i < select_items_.size(); ++i) {
            if (select_items_[i].kind == SelectItemKind::GroupKey) {
                group_key_columns.push_back(i);
            }
        }

        if (group_key_columns.empty()) {
            return;
        }

        std::ranges::sort(rows, [&](const auto& lhs, const auto& rhs) {
            for (const size_t column : group_key_columns) {
                const std::strong_ordering comparison =
                    CompareValues(select_items_[column].output_type, lhs[column], rhs[column]);
                if (comparison != 0) {
                    return comparison < 0;
                }
            }

            return false;
        });
    }

    std::unique_ptr<Operator> child_;
    GroupKeyEncoder group_key_encoder_;
    std::vector<GroupAggBinding> bindings_;
    std::vector<PlannedSelectItem> select_items_;
    std::unordered_map<std::string, size_t> group_indexes_;
    std::vector<GroupState> groups_;
    bool sort_by_group_keys_ = false;
    bool returned_ = false;
};

struct SortedRow {
    std::vector<std::string> values;
    size_t ordinal = 0;
};

class RowOrdering {
   public:
    RowOrdering(std::vector<PlannedOrderBy> order_by, std::vector<PlannedSelectItem> select_items)
        : order_by_(std::move(order_by)), select_items_(std::move(select_items)) {}

    bool operator()(const SortedRow& lhs, const SortedRow& rhs) const {
        for (const auto& order : order_by_) {
            const std::strong_ordering comparison = CompareValues(
                order.value_type, lhs.values[order.result_column_index], rhs.values[order.result_column_index]);
            if (comparison != 0) {
                return order.descending ? comparison > 0 : comparison < 0;
            }
        }

        return lhs.ordinal < rhs.ordinal;
    }

   private:
    std::vector<PlannedOrderBy> order_by_;
    std::vector<PlannedSelectItem> select_items_;
};

static std::vector<std::string> MaterializeBatchRow(const Batch& batch, const size_t row) {
    std::vector<std::string> values;
    values.reserve(batch.ColumnsCount());

    for (size_t column_index = 0; column_index < batch.ColumnsCount(); ++column_index) {
        values.push_back(batch.ColumnAt(column_index).ValueAsString(row));
    }

    return values;
}

static Batch BuildBatchFromSortedRows(const Schema& schema, const std::vector<SortedRow>& rows) {
    Batch result(schema, rows.size());

    for (const auto& row : rows) {
        for (size_t i = 0; i < row.values.size(); ++i) {
            result.AppendValueFromString(i, row.values[i]);
        }
    }

    return result;
}

class OrderByOperator final : public Operator {
   public:
    OrderByOperator(std::unique_ptr<Operator> child, std::vector<PlannedOrderBy> order_by,
                    std::vector<PlannedSelectItem> select_items)
        : child_(std::move(child)), ordering_(std::move(order_by), std::move(select_items)) {}

    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }

        returned_ = true;

        std::optional<Schema> schema;
        std::vector<SortedRow> rows;
        size_t ordinal = 0;

        while (auto batch = child_->Next()) {
            if (!schema.has_value()) {
                schema = batch->GetSchema();
            }

            rows.reserve(rows.size() + batch->RowsCount());

            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                rows.push_back(SortedRow{
                    .values = MaterializeBatchRow(*batch, row),
                    .ordinal = ordinal++,
                });
            }
        }

        if (!schema.has_value()) {
            return std::nullopt;
        }

        SortRows(rows);

        return BuildBatchFromSortedRows(*schema, rows);
    }

   private:
    void SortRows(std::vector<SortedRow>& rows) const { std::ranges::sort(rows, ordering_); }

    std::unique_ptr<Operator> child_;
    RowOrdering ordering_;
    bool returned_ = false;
};

class TopKOperator final : public Operator {
   public:
    TopKOperator(std::unique_ptr<Operator> child, std::vector<PlannedOrderBy> order_by,
                 std::vector<PlannedSelectItem> select_items, const size_t limit)
        : child_(std::move(child)), ordering_(std::move(order_by), std::move(select_items)), limit_(limit) {}

    std::optional<Batch> Next() override {
        if (returned_ || limit_ == 0) {
            return std::nullopt;
        }

        returned_ = true;

        std::optional<Schema> schema;
        std::vector<SortedRow> rows;
        size_t ordinal = 0;

        while (auto batch = child_->Next()) {
            if (!schema.has_value()) {
                schema = batch->GetSchema();
            }

            rows.reserve(rows.size() + batch->RowsCount());

            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                rows.push_back(SortedRow{
                    .values = MaterializeBatchRow(*batch, row),
                    .ordinal = ordinal++,
                });
            }
        }

        if (!schema.has_value()) {
            return std::nullopt;
        }

        std::ranges::sort(rows, ordering_);
        if (rows.size() > limit_) {
            rows.resize(limit_);
        }

        return BuildBatchFromSortedRows(*schema, rows);
    }

   private:
    std::unique_ptr<Operator> child_;
    RowOrdering ordering_;
    size_t limit_;
    bool returned_ = false;
};

class LimitOperator final : public Operator {
   public:
    LimitOperator(std::unique_ptr<Operator> child, const size_t limit) : child_(std::move(child)), remaining_(limit) {}

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
                    limited.AppendValueFromString(column_index, batch->ColumnAt(column_index).ValueAsString(row));
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
                                                  planned.select_items, planned.order_by.empty());
    } else {
        root = std::make_unique<AggOperator>(std::move(root), planned.aggregates);
    }

    if (!planned.order_by.empty() && planned.limit.has_value()) {
        root = std::make_unique<TopKOperator>(std::move(root), planned.order_by, planned.select_items, *planned.limit);
    } else if (!planned.order_by.empty()) {
        root = std::make_unique<OrderByOperator>(std::move(root), planned.order_by, planned.select_items);
    } else if (planned.limit.has_value()) {
        root = std::make_unique<LimitOperator>(std::move(root), *planned.limit);
    }

    return root;
}
