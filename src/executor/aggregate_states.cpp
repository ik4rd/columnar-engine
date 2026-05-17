#include <algorithm>
#include <array>
#include <cctype>
#include <unordered_set>
#include <utility>

#include "common/ascii.h"
#include "common/error.h"
#include "common/int128.h"
#include "common/parsing.h"
#include "executor/aggregate_function.h"
#include "executor/aggregate_state.h"
#include "model/column_traits.h"

static bool ShouldReplaceExtremum(const ColumnType type, const std::string_view candidate,
                                  const std::string_view current, const bool is_min) {
    return VisitColumnType(type, [&]<ColumnType TypeValue>() {
        const auto left = ColumnValueTraits<TypeValue>::Parse(std::string(candidate));
        const auto right = ColumnValueTraits<TypeValue>::Parse(std::string(current));

        return is_min ? left < right : left > right;
    });
}

static Int128 ParseToInt128(const ColumnType type, const std::string_view value) {
    switch (type) {
        case ColumnType::Boolean:
            return ParseBoolean(std::string(value)) ? 1 : 0;
        case ColumnType::Int16:
            return ParseInt16(std::string(value));
        case ColumnType::Int32:
            return ParseInt32(std::string(value));
        case ColumnType::Int64:
            return ParseInt64(std::string(value));
        case ColumnType::Int128:
            return ParseInt128(std::string(value));
        case ColumnType::Character:
            return ParseCharacter(std::string(value));
        case ColumnType::String:
        case ColumnType::Date:
        case ColumnType::Timestamp:
            break;
    }

    throw Error::Unsupported("executor", "unsupported numeric conversion");
}

static std::string FormatAverage(const Int128 sum, const uint64_t count) {
    if (count == 0) {
        return "0";
    }
    return Int128ToString(sum / static_cast<Int128>(count));
}

class CountAS final : public AggState {
   public:
    void ConsumeValue(const std::string_view) override { ConsumeRow(); }
    void ConsumeRow() override { ++count_; }

    std::string Finalize() const override { return std::to_string(count_); }

   private:
    uint64_t count_ = 0;
};

class SumAS final : public AggState {
   public:
    explicit SumAS(const ColumnType type) : type_(type) {}

    void ConsumeValue(const std::string_view value) override { sum_ += ParseToInt128(type_, value); }
    void ConsumeRow() override { throw Error::InvalidState("executor", "SUM requires a value"); }

    std::string Finalize() const override { return Int128ToString(sum_); }

   private:
    ColumnType type_;
    Int128 sum_ = 0;
};

class AvgAS final : public AggState {
   public:
    explicit AvgAS(const ColumnType type) : type_(type) {}

    void ConsumeValue(const std::string_view value) override {
        sum_ += ParseToInt128(type_, value);
        ++count_;
    }
    void ConsumeRow() override { throw Error::InvalidState("executor", "AVG requires a value"); }

    std::string Finalize() const override { return FormatAverage(sum_, count_); }

   private:
    ColumnType type_;
    Int128 sum_ = 0;
    uint64_t count_ = 0;
};

class ExtremumAS final : public AggState {
   public:
    ExtremumAS(const ColumnType type, const bool is_min) : type_(type), is_min_(is_min) {}

    void ConsumeValue(const std::string_view value) override {
        if (!extremum_.has_value()) {
            extremum_ = std::string(value);
            return;
        }

        if (ShouldReplaceExtremum(type_, value, *extremum_, is_min_)) {
            extremum_ = std::string(value);
        }
    }
    void ConsumeRow() override { throw Error::InvalidState("executor", "MIN/MAX requires a value"); }

    std::string Finalize() const override { return extremum_.value_or(""); }

   private:
    ColumnType type_;
    bool is_min_ = true;
    std::optional<std::string> extremum_;
};

class DistinctAS final : public AggState {
   public:
    explicit DistinctAS(std::unique_ptr<AggState> nested) : nested_(std::move(nested)) {}

    void ConsumeValue(const std::string_view value) override {
        if (seen_.insert(std::string(value)).second) {
            nested_->ConsumeValue(value);
        }
    }
    void ConsumeRow() override { throw Error::InvalidState("executor", "DISTINCT requires a value"); }

    std::string Finalize() const override { return nested_->Finalize(); }

   private:
    std::unique_ptr<AggState> nested_;
    std::unordered_set<std::string> seen_;
};

static bool SupportsAnyType(const ColumnType) { return true; }

static bool SupportsNumericType(const ColumnType type) {
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

static std::unique_ptr<AggState> CreateCountState(const PlannedAgg&) { return std::make_unique<CountAS>(); }

static std::unique_ptr<AggState> CreateSumState(const PlannedAgg& aggregate) {
    return std::make_unique<SumAS>(aggregate.input_type);
}

static std::unique_ptr<AggState> CreateAvgState(const PlannedAgg& aggregate) {
    return std::make_unique<AvgAS>(aggregate.input_type);
}

static std::unique_ptr<AggState> CreateMinState(const PlannedAgg& aggregate) {
    return std::make_unique<ExtremumAS>(aggregate.input_type, true);
}

static std::unique_ptr<AggState> CreateMaxState(const PlannedAgg& aggregate) {
    return std::make_unique<ExtremumAS>(aggregate.input_type, false);
}

AggRegistry& AggRegistry::Instance() {
    static AggRegistry instance;
    return instance;
}

void AggRegistry::Register(const AggFuncDefinition& def) { registry_[ToUpperAscii(def.canonical_name)] = def; }

const AggFuncDefinition* AggRegistry::Find(const std::string_view name) const {
    const auto it = registry_.find(ToUpperAscii(name));
    if (it == registry_.end()) {
        return nullptr;
    }
    return &it->second;
}

AggRegistrar::AggRegistrar(const AggFuncDefinition& def) { AggRegistry::Instance().Register(def); }

[[maybe_unused]] static AggRegistrar register_count({
    .canonical_name = "COUNT",
    .call_features = AggCallFeature::Distinct | AggCallFeature::Star,
    .supports_type = &SupportsAnyType,
    .factory = &CreateCountState,
});

[[maybe_unused]] static AggRegistrar register_sum({
    .canonical_name = "SUM",
    .supports_type = &SupportsNumericType,
    .factory = &CreateSumState,
});

[[maybe_unused]] static AggRegistrar register_avg({
    .canonical_name = "AVG",
    .supports_type = &SupportsNumericType,
    .factory = &CreateAvgState,
});

[[maybe_unused]] static AggRegistrar register_min({
    .canonical_name = "MIN",
    .supports_type = &SupportsAnyType,
    .factory = &CreateMinState,
});

[[maybe_unused]] static AggRegistrar register_max({
    .canonical_name = "MAX",
    .supports_type = &SupportsAnyType,
    .factory = &CreateMaxState,
});

const AggFuncDefinition& ResolveAggFunc(const std::string_view name) {
    const AggFuncDefinition* definition = AggRegistry::Instance().Find(name);
    if (definition == nullptr) {
        throw Error::Unsupported("executor", "aggregate '" + std::string(name) + "' is not registered");
    }
    return *definition;
}

bool AggSupportsInputType(const AggFuncDefinition& definition, const ColumnType type) {
    if (definition.supports_type == nullptr) {
        return false;
    }
    return definition.supports_type(type);
}

std::unique_ptr<AggState> CreateAggState(const PlannedAgg& aggregate) {
    if (aggregate.function == nullptr || aggregate.function->factory == nullptr) {
        throw Error::Unsupported("executor", "aggregate is not registered");
    }

    if (aggregate.distinct && !HasAggCallFeature(aggregate.function->call_features, AggCallFeature::Distinct)) {
        throw Error::Unsupported("executor", "DISTINCT is not supported for aggregate '" +
                                                 std::string(aggregate.function->canonical_name) + "'");
    }

    if (aggregate.argument_kind == AggArgumentKind::Star &&
        !HasAggCallFeature(aggregate.function->call_features, AggCallFeature::Star)) {
        throw Error::Unsupported(
            "executor", "aggregate '" + std::string(aggregate.function->canonical_name) + "' does not support '*'");
    }

    std::unique_ptr<AggState> state = aggregate.function->factory(aggregate);
    if (aggregate.distinct) {
        state = std::make_unique<DistinctAS>(std::move(state));
    }

    return state;
}
