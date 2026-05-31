#include <algorithm>
#include <chrono>
#include <compare>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/ascii.h"
#include "common/error.h"
#include "common/parsing.h"
#include "common/string_arena.h"
#include "executor/aggregate_function.h"
#include "executor/aggregate_state.h"
#include "executor/comparison_utils.h"
#include "executor/operators_internal.h"
#include "executor/typed_value_utils.h"

constexpr std::string_view ExtractMinutePart = "MINUTE";
constexpr std::string_view ExtractHourPart = "HOUR";
constexpr int64_t MinuteMicros = 60'000'000;

template <typename Binding>
Schema BuildAggregateOutputSchema(const std::vector<Binding>& bindings) {
    Schema schema;
    schema.columns.reserve(bindings.size());

    for (const auto& binding : bindings) {
        schema.columns.push_back(ColumnSchema(binding.aggregate.output_name, AggregateOutputType(binding.aggregate)));
    }

    return schema;
}

std::optional<Int128> TryEvalTypedGroupKeyInt(const ExprPtr& expr, const Batch& batch, size_t row);

bool UsesRowAggInput(const PlannedAgg& aggregate) {
    return !aggregate.direct_numeric_argument && aggregate.argument_kind != AggArgumentKind::Column;
}

void ConsumeAggRow(const PlannedAgg& aggregate, const Batch& batch, const size_t row, AggState& state) {
    if (aggregate.direct_numeric_argument) {
        state.ConsumeInt128(batch.ColumnAt(aggregate.column_index).ValueAsInt128(row) +
                            aggregate.direct_numeric_offset);
        return;
    }

    if (aggregate.argument_kind == AggArgumentKind::Column) {
        if (aggregate.input_type != ColumnType::String) {
            if (const auto typed_value = TryEvalTypedGroupKeyInt(aggregate.argument, batch, row);
                typed_value.has_value()) {
                state.ConsumeInt128(*typed_value);
                return;
            }
        }
        state.ConsumeValue(EvalExpr(aggregate.argument, batch, row));
        return;
    }

    state.ConsumeRow();
}

void ConsumeAggRows(const PlannedAgg& aggregate, const Batch& batch, const std::span<const size_t> rows,
                    AggState& state) {
    if (UsesRowAggInput(aggregate)) {
        state.ConsumeRows(rows.size());
        return;
    }

    for (const size_t row : rows) {
        ConsumeAggRow(aggregate, batch, row, state);
    }
}

struct AggBinding {
    PlannedAgg aggregate;

    std::unique_ptr<AggState> state;
};

class CompactAggState {
   public:
    explicit CompactAggState(const PlannedAgg& aggregate) : type_(aggregate.input_type) {
        if (aggregate.distinct || aggregate.function == nullptr) {
            fallback_ = CreateAggState(aggregate);
            return;
        }

        const std::string name = ToUpperAscii(aggregate.function->canonical_name);
        if (name == "COUNT") {
            kind_ = Kind::Count;
        } else if (name == "SUM") {
            kind_ = Kind::Sum;
        } else if (name == "AVG") {
            kind_ = Kind::Avg;
        } else if (name == "MIN") {
            kind_ = Kind::Extremum;
            is_min_ = true;
        } else if (name == "MAX") {
            kind_ = Kind::Extremum;
            is_min_ = false;
        } else {
            fallback_ = CreateAggState(aggregate);
        }
    }

    void ConsumeValue(const std::string_view value) {
        if (fallback_) {
            fallback_->ConsumeValue(value);
            return;
        }

        switch (kind_) {
            case Kind::Count:
                ConsumeRow();
                return;
            case Kind::Sum:
                sum_ += ParseColumnValueAsInt128(type_, value);
                return;
            case Kind::Avg:
                sum_ += ParseColumnValueAsInt128(type_, value);
                ++count_;
                return;
            case Kind::Extremum:
                ConsumeExtremum(value);
        }
    }

    void ConsumeInt128(const Int128 value) {
        if (fallback_) {
            fallback_->ConsumeInt128(value);
            return;
        }

        switch (kind_) {
            case Kind::Count:
                ConsumeRow();
                return;
            case Kind::Sum:
                sum_ += value;
                return;
            case Kind::Avg:
                sum_ += value;
                ++count_;
                return;
            case Kind::Extremum:
                if (!typed_extremum_.has_value() || (is_min_ ? value < *typed_extremum_ : value > *typed_extremum_)) {
                    typed_extremum_ = value;
                }
        }
    }

    void ConsumeRow() {
        if (fallback_) {
            fallback_->ConsumeRow();
            return;
        }

        if (kind_ != Kind::Count) {
            throw Error::InvalidState("executor", "aggregate requires a value");
        }

        ++count_;
    }

    void ConsumeRows(const size_t count) {
        if (fallback_) {
            fallback_->ConsumeRows(count);
            return;
        }

        if (kind_ != Kind::Count) {
            throw Error::InvalidState("executor", "aggregate requires a value");
        }

        count_ += count;
    }

    std::string Finalize() const {
        if (fallback_) {
            return fallback_->Finalize();
        }

        switch (kind_) {
            case Kind::Count:
                return std::to_string(count_);
            case Kind::Sum:
                return Int128ToString(sum_);
            case Kind::Avg:
                return count_ == 0 ? "0" : Int128ToString(sum_ / static_cast<Int128>(count_));
            case Kind::Extremum:
                if (typed_extremum_.has_value()) {
                    return FormatInt128Value(type_, *typed_extremum_);
                }
                return string_extremum_.value_or("");
        }

        return {};
    }

    Int128 FinalizeInt(const ColumnType output_type) const {
        if (fallback_) {
            return ParseColumnValueAsInt128(output_type, fallback_->Finalize());
        }

        switch (kind_) {
            case Kind::Count:
                return count_;
            case Kind::Sum:
                return sum_;
            case Kind::Avg:
                return count_ == 0 ? 0 : sum_ / static_cast<Int128>(count_);
            case Kind::Extremum:
                return typed_extremum_.value_or(0);
        }

        return 0;
    }

   private:
    enum class Kind {
        Count,
        Sum,
        Avg,
        Extremum,
    };

    void ConsumeExtremum(const std::string_view value) {
        if (IsNumericColumnType(type_) || type_ == ColumnType::Date || type_ == ColumnType::Timestamp ||
            type_ == ColumnType::Boolean || type_ == ColumnType::Character) {
            ConsumeInt128(ParseColumnValueAsInt128(type_, value));
            return;
        }

        if (!string_extremum_.has_value() ||
            (is_min_ ? value < std::string_view(*string_extremum_) : value > std::string_view(*string_extremum_))) {
            string_extremum_ = std::string(value);
        }
    }

    Kind kind_ = Kind::Count;
    ColumnType type_ = ColumnType::String;
    bool is_min_ = true;

    Int128 sum_ = 0;
    uint64_t count_ = 0;

    std::optional<Int128> typed_extremum_;
    std::optional<std::string> string_extremum_;
    std::unique_ptr<AggState> fallback_;
};

void ConsumeCompactAggRow(const PlannedAgg& aggregate, const Batch& batch, const size_t row, CompactAggState& state) {
    if (aggregate.direct_numeric_argument) {
        state.ConsumeInt128(batch.ColumnAt(aggregate.column_index).ValueAsInt128(row) +
                            aggregate.direct_numeric_offset);
        return;
    }

    if (aggregate.argument_kind == AggArgumentKind::Column) {
        if (aggregate.input_type != ColumnType::String) {
            if (const auto typed_value = TryEvalTypedGroupKeyInt(aggregate.argument, batch, row);
                typed_value.has_value()) {
                state.ConsumeInt128(*typed_value);
                return;
            }
        }
        state.ConsumeValue(EvalExpr(aggregate.argument, batch, row));
        return;
    }

    state.ConsumeRow();
}

struct FinalizedAggregateValues {
    std::vector<std::string> values;
    std::vector<Int128> int_values;
};

struct GroupKeyComponent {
    ColumnType type = ColumnType::String;

    Int128 int_value = 0;
    std::string_view string_value;
};

std::optional<Int128> TryEvalTypedGroupKeyInt(const ExprPtr& expr, const Batch& batch, const size_t row) {
    if (!expr) {
        return std::nullopt;
    }

    switch (expr->kind) {
        case ExprKind::Column:
            if (!expr->column_index_bound || expr->column_index >= batch.ColumnsCount()) {
                return std::nullopt;
            }
            if (batch.ColumnAt(expr->column_index).Type() == ColumnType::String) {
                return std::nullopt;
            }
            return batch.ColumnAt(expr->column_index).ValueAsInt128(row);
        case ExprKind::Literal:
            if (expr->literal.kind != LiteralKind::Numeric) {
                return std::nullopt;
            }
            return ParseInt128(expr->literal.text);
        case ExprKind::Binary: {
            const auto lhs = TryEvalTypedGroupKeyInt(expr->left, batch, row);
            const auto rhs = TryEvalTypedGroupKeyInt(expr->right, batch, row);
            if (!lhs.has_value() || !rhs.has_value()) {
                return std::nullopt;
            }
            return expr->binary_op == BinaryOp::Add ? *lhs + *rhs : *lhs - *rhs;
        }
        case ExprKind::Function: {
            const std::string name = ToUpperAscii(expr->function_name);

            if (name == "EXTRACT" && expr->arguments.size() == 2 && expr->arguments[0] &&
                expr->arguments[0]->kind == ExprKind::Literal) {
                const auto ts = TryEvalTypedGroupKeyInt(expr->arguments[1], batch, row);
                if (!ts.has_value()) {
                    return std::nullopt;
                }

                const std::string part = ToUpperAscii(expr->arguments[0]->literal.text);
                const std::chrono::sys_time<std::chrono::microseconds> time{
                    std::chrono::microseconds{static_cast<int64_t>(*ts)}};
                const auto day = std::chrono::floor<std::chrono::days>(time);
                const std::chrono::hh_mm_ss tod{time - day};

                if (part == ExtractMinutePart) {
                    return tod.minutes().count();
                }
                if (part == ExtractHourPart) {
                    return tod.hours().count();
                }
            }

            if (name == "DATE_TRUNC" && expr->arguments.size() == 2 && expr->arguments[0] &&
                expr->arguments[0]->kind == ExprKind::Literal) {
                const auto ts = TryEvalTypedGroupKeyInt(expr->arguments[1], batch, row);
                if (!ts.has_value()) {
                    return std::nullopt;
                }

                const std::string part = ToUpperAscii(NormalizeLiteralForEval(expr->arguments[0]->literal));
                if (part == ExtractMinutePart) {
                    const int64_t micros = static_cast<int64_t>(*ts);
                    return micros / MinuteMicros * MinuteMicros;
                }
            }

            return std::nullopt;
        }
        case ExprKind::Case:
        case ExprKind::Star:
            return std::nullopt;
    }

    return std::nullopt;
}

std::string FormatGroupKeyValue(const GroupKeyComponent& value) {
    switch (value.type) {
        case ColumnType::Boolean:
        case ColumnType::Int16:
        case ColumnType::Int32:
        case ColumnType::Int64:
        case ColumnType::Int128:
        case ColumnType::Date:
        case ColumnType::Timestamp:
        case ColumnType::Character:
            return FormatInt128Value(value.type, value.int_value);
        case ColumnType::String:
            return std::string(value.string_value);
    }

    return {};
}

std::strong_ordering CompareGroupKeyValue(const GroupKeyComponent& lhs, const GroupKeyComponent& rhs) {
    if (lhs.type != rhs.type) {
        throw Error::InconsistentData("executor", "group key type mismatch");
    }

    if (lhs.type == ColumnType::String) {
        return lhs.string_value <=> rhs.string_value;
    }

    return lhs.int_value <=> rhs.int_value;
}

bool GroupKeyValueEquals(const GroupKeyComponent& lhs, const GroupKeyComponent& rhs) {
    return CompareGroupKeyValue(lhs, rhs) == 0;
}

size_t HashCombine(const size_t seed, const size_t value) {
    return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

size_t HashInt128(const Int128 value) {
    const auto bits = static_cast<UInt128>(value);
    const uint64_t low = static_cast<uint64_t>(bits);
    const uint64_t high = static_cast<uint64_t>(bits >> 64);

    return HashCombine(std::hash<uint64_t>{}(high), std::hash<uint64_t>{}(low));
}

struct TypedGroupKey {
    std::vector<GroupKeyComponent> values;
};

struct TypedGroupKeyHash {
    size_t operator()(const TypedGroupKey& key) const {
        size_t hash = 0;

        for (const GroupKeyComponent& value : key.values) {
            hash = HashCombine(hash, std::hash<int>{}(static_cast<int>(value.type)));
            hash = HashCombine(hash, value.type == ColumnType::String ? StringViewHash{}(value.string_value)
                                                                      : HashInt128(value.int_value));
        }

        return hash;
    }
};

struct TypedGroupKeyEqual {
    bool operator()(const TypedGroupKey& lhs, const TypedGroupKey& rhs) const {
        if (lhs.values.size() != rhs.values.size()) {
            return false;
        }

        for (size_t i = 0; i < lhs.values.size(); ++i) {
            if (!GroupKeyValueEquals(lhs.values[i], rhs.values[i])) {
                return false;
            }
        }

        return true;
    }
};

class GroupKeyMaterializer {
   public:
    explicit GroupKeyMaterializer(std::vector<PlannedGroupKey> group_keys) : group_keys_(std::move(group_keys)) {}

    TypedGroupKey Materialize(const Batch& batch, const size_t row, StringArena& arena) const {
        TypedGroupKey key;
        key.values.reserve(group_keys_.size());

        for (const auto& group_key : group_keys_) {
            if (group_key.column_type != ColumnType::String) {
                if (const auto typed_value = TryEvalTypedGroupKeyInt(group_key.expression, batch, row);
                    typed_value.has_value()) {
                    key.values.push_back(GroupKeyComponent{
                        .type = group_key.column_type,
                        .int_value = *typed_value,
                        .string_value = {},
                    });
                    continue;
                }
            } else if (group_key.expression && group_key.expression->kind == ExprKind::Column &&
                       group_key.expression->column_index_bound &&
                       group_key.expression->column_index < batch.ColumnsCount()) {
                key.values.push_back(GroupKeyComponent{
                    .type = group_key.column_type,
                    .int_value = 0,
                    .string_value = arena.Store(batch.ColumnAt(group_key.expression->column_index).ValueAsString(row)),
                });
                continue;
            }

            std::string value = EvalExpr(group_key.expression, batch, row);
            if (group_key.column_type == ColumnType::String) {
                key.values.push_back(GroupKeyComponent{
                    .type = group_key.column_type,
                    .int_value = 0,
                    .string_value = arena.Store(value),
                });
            } else {
                key.values.push_back(GroupKeyComponent{
                    .type = group_key.column_type,
                    .int_value = ParseColumnValueAsInt128(group_key.column_type, value),
                    .string_value = {},
                });
            }
        }

        return key;
    }

   private:
    std::vector<PlannedGroupKey> group_keys_;
};

class AggOperator final : public Operator {
   public:
    AggOperator(std::unique_ptr<Operator> child, std::vector<PlannedAgg> aggregates, PredicatePtr filter)
        : child_(std::move(child)), filter_(std::move(filter)) {
        bindings_.reserve(aggregates.size());

        for (auto& aggregate : aggregates) {
            std::unique_ptr<AggState> state = CreateAggState(aggregate);
            bindings_.push_back(AggBinding{
                .aggregate = std::move(aggregate),
                .state = std::move(state),
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
                if (!EvaluatePredicate(filter_, *batch, row)) {
                    continue;
                }
                for (auto& binding : bindings_) {
                    ConsumeAggRow(binding.aggregate, *batch, row, *binding.state);
                }
            }
        }

        Batch result(BuildAggregateOutputSchema(bindings_));

        for (size_t i = 0; i < bindings_.size(); ++i) {
            result.AppendValueFromString(i, bindings_[i].state->Finalize());
        }

        return result;
    }

   private:
    std::unique_ptr<Operator> child_;
    PredicatePtr filter_;

    std::vector<AggBinding> bindings_;

    bool returned_ = false;
};

class GroupAggOperator final : public Operator {
   public:
    GroupAggOperator(std::unique_ptr<Operator> child, std::vector<PlannedGroupKey> group_keys,
                     const std::vector<PlannedAgg>& aggregates, std::vector<PlannedSelectItem> select_items,
                     PredicatePtr filter, PredicatePtr having, const bool sort_by_group_keys)
        : child_(std::move(child)),
          group_key_materializer_(std::move(group_keys)),
          select_items_(std::move(select_items)),
          filter_(std::move(filter)),
          having_(std::move(having)),
          sort_by_group_keys_(sort_by_group_keys) {
        bindings_.reserve(aggregates.size());

        for (const auto& aggregate : aggregates) {
            bindings_.push_back(GroupAggBinding{
                .aggregate = aggregate,
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
                if (!EvaluatePredicate(filter_, *batch, row)) {
                    continue;
                }

                TypedGroupKey key = group_key_materializer_.Materialize(*batch, row, string_arena_);

                auto it = groups_.find(key);
                if (it == groups_.end()) {
                    GroupState group{
                        .states = CreateStates(),
                        .finalized_aggregates = nullptr,
                        .ordinal = next_group_ordinal_++,
                    };
                    it = groups_.emplace(std::move(key), std::move(group)).first;
                }

                GroupState& group = it->second;
                for (size_t i = 0; i < bindings_.size(); ++i) {
                    ConsumeCompactAggRow(bindings_[i].aggregate, *batch, row, group.states[i]);
                }
            }
        }

        const Schema schema = BuildSelectOutputSchema(select_items_);
        const std::vector<const GroupEntry*> output_order = BuildOutputOrder();
        Batch result(schema, groups_.size());

        for (const GroupEntry* entry : output_order) {
            if (GroupMatchesHaving(entry->first, entry->second, schema)) {
                AppendGroupToBatch(entry->first, entry->second, result);
            }
        }

        return result;
    }

   private:
    struct GroupState {
        std::vector<CompactAggState> states;

        mutable std::unique_ptr<FinalizedAggregateValues> finalized_aggregates;

        size_t ordinal = 0;
    };

    using GroupMap = std::unordered_map<TypedGroupKey, GroupState, TypedGroupKeyHash, TypedGroupKeyEqual>;
    using GroupEntry = GroupMap::value_type;

    struct GroupAggBinding {
        PlannedAgg aggregate;
    };

    std::vector<CompactAggState> CreateStates() const {
        std::vector<CompactAggState> states;
        states.reserve(bindings_.size());

        for (const auto& binding : bindings_) {
            states.emplace_back(binding.aggregate);
        }

        return states;
    }

    const FinalizedAggregateValues& FinalizeAggregateValues(const GroupState& group) const {
        if (group.finalized_aggregates) {
            return *group.finalized_aggregates;
        }

        auto finalized = std::make_unique<FinalizedAggregateValues>();
        finalized->values.reserve(group.states.size());
        finalized->int_values.reserve(group.states.size());

        for (size_t i = 0; i < group.states.size(); ++i) {
            const ColumnType type = AggregateOutputType(bindings_[i].aggregate);
            if (type == ColumnType::String) {
                finalized->int_values.push_back(0);
                finalized->values.push_back(group.states[i].Finalize());
                continue;
            }

            const Int128 value = group.states[i].FinalizeInt(type);
            finalized->int_values.push_back(value);
            finalized->values.push_back(FormatInt128Value(type, value));
        }

        group.finalized_aggregates = std::move(finalized);
        return *group.finalized_aggregates;
    }

    void AppendGroupToBatch(const TypedGroupKey& key, const GroupState& group, const Batch& batch) const {
        const FinalizedAggregateValues& finalized = FinalizeAggregateValues(group);

        for (size_t column = 0; column < select_items_.size(); ++column) {
            const PlannedSelectItem& item = select_items_[column];
            if (item.kind == SelectItemKind::GroupKey) {
                batch.AppendValueFromString(column, FormatGroupKeyValue(key.values[item.index]));
            } else {
                batch.AppendValueFromString(column, finalized.values[item.index]);
            }
        }
    }

    bool GroupMatchesHaving(const TypedGroupKey& key, const GroupState& group, const Schema& schema) const {
        if (!having_) {
            return true;
        }

        const Batch row(schema, 1);
        AppendGroupToBatch(key, group, row);

        return EvaluatePredicate(having_, row, 0);
    }

    std::vector<const GroupEntry*> BuildOutputOrder() const {
        std::vector<const GroupEntry*> order;
        order.reserve(groups_.size());

        for (const auto& entry : groups_) {
            order.push_back(&entry);
        }

        if (!sort_by_group_keys_) {
            std::ranges::sort(order, [](const GroupEntry* lhs, const GroupEntry* rhs) {
                return lhs->second.ordinal < rhs->second.ordinal;
            });
            return order;
        }

        std::vector<size_t> group_key_columns;

        for (size_t i = 0; i < select_items_.size(); ++i) {
            if (select_items_[i].kind == SelectItemKind::GroupKey) {
                group_key_columns.push_back(i);
            }
        }

        if (group_key_columns.empty()) {
            return order;
        }

        std::ranges::sort(order, [&](const GroupEntry* lhs, const GroupEntry* rhs) {
            for (const size_t column : group_key_columns) {
                const PlannedSelectItem& item = select_items_[column];
                const std::strong_ordering comparison =
                    CompareGroupKeyValue(lhs->first.values[item.index], rhs->first.values[item.index]);
                if (comparison != 0) {
                    return comparison < 0;
                }
            }

            return lhs->second.ordinal < rhs->second.ordinal;
        });

        return order;
    }

    std::unique_ptr<Operator> child_;
    GroupKeyMaterializer group_key_materializer_;

    std::vector<GroupAggBinding> bindings_;
    std::vector<PlannedSelectItem> select_items_;

    PredicatePtr filter_;
    PredicatePtr having_;

    StringArena string_arena_;
    GroupMap groups_;
    size_t next_group_ordinal_ = 0;

    bool sort_by_group_keys_ = false;
    bool returned_ = false;
};

class GroupAggTopKOperator final : public Operator {
   public:
    GroupAggTopKOperator(std::unique_ptr<Operator> child, std::vector<PlannedGroupKey> group_keys,
                         const std::vector<PlannedAgg>& aggregates, std::vector<PlannedSelectItem> select_items,
                         PredicatePtr filter, std::vector<PlannedOrderBy> order_by, const size_t limit)
        : child_(std::move(child)),
          group_key_materializer_(std::move(group_keys)),
          select_items_(std::move(select_items)),
          filter_(std::move(filter)),
          order_by_(std::move(order_by)),
          limit_(limit) {
        bindings_.reserve(aggregates.size());

        for (const auto& aggregate : aggregates) {
            bindings_.push_back(GroupAggBinding{
                .aggregate = aggregate,
            });
        }
    }

    std::optional<Batch> Next() override {
        if (returned_ || limit_ == 0) {
            return std::nullopt;
        }

        returned_ = true;

        while (auto batch = child_->Next()) {
            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                if (!EvaluatePredicate(filter_, *batch, row)) {
                    continue;
                }

                TypedGroupKey key = group_key_materializer_.Materialize(*batch, row, string_arena_);

                auto it = groups_.find(key);
                if (it == groups_.end()) {
                    GroupState group{
                        .states = CreateStates(),
                        .finalized_aggregates = nullptr,
                        .ordinal = next_group_ordinal_++,
                    };
                    it = groups_.emplace(std::move(key), std::move(group)).first;
                }

                GroupState& group = it->second;
                for (size_t i = 0; i < bindings_.size(); ++i) {
                    ConsumeCompactAggRow(bindings_[i].aggregate, *batch, row, group.states[i]);
                }
            }
        }

        const Schema schema = BuildSelectOutputSchema(select_items_);
        const std::vector<const GroupEntry*> top_groups = BuildTopGroups();
        Batch result(schema, top_groups.size());

        for (const GroupEntry* entry : top_groups) {
            AppendGroupToBatch(entry->first, entry->second, result);
        }

        return result;
    }

   private:
    struct GroupState {
        std::vector<CompactAggState> states;

        mutable std::unique_ptr<FinalizedAggregateValues> finalized_aggregates;
        size_t ordinal = 0;
    };

    using GroupMap = std::unordered_map<TypedGroupKey, GroupState, TypedGroupKeyHash, TypedGroupKeyEqual>;
    using GroupEntry = GroupMap::value_type;

    struct GroupAggBinding {
        PlannedAgg aggregate;
    };

    std::vector<CompactAggState> CreateStates() const {
        std::vector<CompactAggState> states;
        states.reserve(bindings_.size());

        for (const auto& binding : bindings_) {
            states.emplace_back(binding.aggregate);
        }

        return states;
    }

    const FinalizedAggregateValues& FinalizeAggregateValues(const GroupState& group) const {
        if (group.finalized_aggregates) {
            return *group.finalized_aggregates;
        }

        auto finalized = std::make_unique<FinalizedAggregateValues>();
        finalized->values.reserve(group.states.size());
        finalized->int_values.reserve(group.states.size());

        for (size_t i = 0; i < group.states.size(); ++i) {
            const ColumnType type = AggregateOutputType(bindings_[i].aggregate);
            if (type == ColumnType::String) {
                finalized->int_values.push_back(0);
                finalized->values.push_back(group.states[i].Finalize());
                continue;
            }

            const Int128 value = group.states[i].FinalizeInt(type);
            finalized->int_values.push_back(value);
            finalized->values.push_back(FormatInt128Value(type, value));
        }

        group.finalized_aggregates = std::move(finalized);
        return *group.finalized_aggregates;
    }

    bool OrdersBefore(const GroupEntry* lhs, const GroupEntry* rhs) const {
        const FinalizedAggregateValues& lhs_finalized = FinalizeAggregateValues(lhs->second);
        const FinalizedAggregateValues& rhs_finalized = FinalizeAggregateValues(rhs->second);

        for (const PlannedOrderBy& order : order_by_) {
            const PlannedSelectItem& item = select_items_[order.result_column_index];
            std::strong_ordering comparison = std::strong_ordering::equal;

            if (item.kind == SelectItemKind::GroupKey) {
                comparison = CompareGroupKeyValue(lhs->first.values[item.index], rhs->first.values[item.index]);
            } else {
                comparison = item.output_type == ColumnType::String
                                 ? lhs_finalized.values[item.index] <=> rhs_finalized.values[item.index]
                                 : lhs_finalized.int_values[item.index] <=> rhs_finalized.int_values[item.index];
            }

            if (comparison != 0) {
                return order.descending ? comparison > 0 : comparison < 0;
            }
        }

        return lhs->second.ordinal < rhs->second.ordinal;
    }

    void AppendGroupToBatch(const TypedGroupKey& key, const GroupState& group, const Batch& batch) const {
        const FinalizedAggregateValues& finalized = FinalizeAggregateValues(group);

        for (size_t column = 0; column < select_items_.size(); ++column) {
            const PlannedSelectItem& item = select_items_[column];
            if (item.kind == SelectItemKind::GroupKey) {
                batch.AppendValueFromString(column, FormatGroupKeyValue(key.values[item.index]));
            } else {
                batch.AppendValueFromString(column, finalized.values[item.index]);
            }
        }
    }

    std::vector<const GroupEntry*> BuildTopGroups() const {
        std::vector<const GroupEntry*> top_groups;
        top_groups.reserve(limit_);

        for (const auto& entry : groups_) {
            const GroupEntry* group = &entry;
            if (top_groups.size() < limit_) {
                top_groups.push_back(group);
                std::ranges::push_heap(
                    top_groups, [&](const GroupEntry* lhs, const GroupEntry* rhs) { return OrdersBefore(lhs, rhs); });
                continue;
            }

            if (OrdersBefore(group, top_groups.front())) {
                std::ranges::pop_heap(
                    top_groups, [&](const GroupEntry* lhs, const GroupEntry* rhs) { return OrdersBefore(lhs, rhs); });
                top_groups.back() = group;
                std::ranges::push_heap(
                    top_groups, [&](const GroupEntry* lhs, const GroupEntry* rhs) { return OrdersBefore(lhs, rhs); });
            }
        }

        std::ranges::sort(top_groups,
                          [&](const GroupEntry* lhs, const GroupEntry* rhs) { return OrdersBefore(lhs, rhs); });

        return top_groups;
    }

    std::unique_ptr<Operator> child_;
    GroupKeyMaterializer group_key_materializer_;

    std::vector<GroupAggBinding> bindings_;
    std::vector<PlannedSelectItem> select_items_;

    PredicatePtr filter_;

    std::vector<PlannedOrderBy> order_by_;
    size_t limit_ = 0;

    StringArena string_arena_;
    mutable GroupMap groups_;
    size_t next_group_ordinal_ = 0;

    bool returned_ = false;
};

std::unique_ptr<Operator> CreateAggOperator(std::unique_ptr<Operator> child, std::vector<PlannedAgg> aggregates,
                                            PredicatePtr filter) {
    return std::make_unique<AggOperator>(std::move(child), std::move(aggregates), std::move(filter));
}

std::unique_ptr<Operator> CreateGroupAggOperator(std::unique_ptr<Operator> child,
                                                 std::vector<PlannedGroupKey> group_keys,
                                                 const std::vector<PlannedAgg>& aggregates,
                                                 std::vector<PlannedSelectItem> select_items, PredicatePtr filter,
                                                 PredicatePtr having, const bool sort_by_group_keys) {
    return std::make_unique<GroupAggOperator>(std::move(child), std::move(group_keys), aggregates,
                                              std::move(select_items), std::move(filter), std::move(having),
                                              sort_by_group_keys);
}

std::unique_ptr<Operator> CreateGroupAggTopKOperator(std::unique_ptr<Operator> child,
                                                     std::vector<PlannedGroupKey> group_keys,
                                                     const std::vector<PlannedAgg>& aggregates,
                                                     std::vector<PlannedSelectItem> select_items, PredicatePtr filter,
                                                     std::vector<PlannedOrderBy> order_by, const size_t limit) {
    return std::make_unique<GroupAggTopKOperator>(std::move(child), std::move(group_keys), aggregates,
                                                  std::move(select_items), std::move(filter), std::move(order_by),
                                                  limit);
}
