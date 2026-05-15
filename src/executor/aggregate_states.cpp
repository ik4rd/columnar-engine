#include <algorithm>
#include <array>
#include <cctype>
#include <unordered_set>
#include <utility>

#include "executor/aggregate_state.h"
#include "executor/aggregate_function.h"
#include "support/ascii.h"
#include "support/error.h"
#include "support/int128.h"
#include "support/parsing.h"

static bool ShouldReplaceExtremum(const ColumnType type, const std::string_view candidate,
                                  const std::string_view current, const bool is_min) {
    switch (type) {
        case ColumnType::Boolean: {
            const bool left = ParseBoolean(std::string(candidate));
            const bool right = ParseBoolean(std::string(current));
            return is_min ? left < right : left > right;
        }
        case ColumnType::Int16: {
            const int16_t left = ParseInt16(std::string(candidate));
            const int16_t right = ParseInt16(std::string(current));
            return is_min ? left < right : left > right;
        }
        case ColumnType::Int32: {
            const int32_t left = ParseInt32(std::string(candidate));
            const int32_t right = ParseInt32(std::string(current));
            return is_min ? left < right : left > right;
        }
        case ColumnType::Int64: {
            const int64_t left = ParseInt64(std::string(candidate));
            const int64_t right = ParseInt64(std::string(current));
            return is_min ? left < right : left > right;
        }
        case ColumnType::Int128: {
            const Int128 left = ParseInt128(std::string(candidate));
            const Int128 right = ParseInt128(std::string(current));
            return is_min ? left < right : left > right;
        }
        case ColumnType::String:
            return is_min ? candidate < current : candidate > current;
        case ColumnType::Date: {
            const int32_t left = ParseDate(std::string(candidate));
            const int32_t right = ParseDate(std::string(current));
            return is_min ? left < right : left > right;
        }
        case ColumnType::Timestamp: {
            const int64_t left = ParseTimestamp(std::string(candidate));
            const int64_t right = ParseTimestamp(std::string(current));
            return is_min ? left < right : left > right;
        }
        case ColumnType::Character: {
            const char left = ParseCharacter(std::string(candidate));
            const char right = ParseCharacter(std::string(current));
            return is_min ? left < right : left > right;
        }
    }
    throw Error::Unsupported("executor", "unsupported column type in comparison");
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
    void Consume(const std::optional<std::string_view> value) override {
        (void)value;
        ++count_;
    }

    std::string Finalize() const override { return std::to_string(count_); }

   private:
    uint64_t count_ = 0;
};

class SumAS final : public AggState {
   public:
    explicit SumAS(const ColumnType type) : type_(type) {}

   public:
    void Consume(const std::optional<std::string_view> value) override {
        if (!value.has_value()) {
            throw Error::InvalidState("executor", "SUM requires a value");
        }
        sum_ += ParseToInt128(type_, *value);
    }

    std::string Finalize() const override { return Int128ToString(sum_); }

   private:
    ColumnType type_;
    Int128 sum_ = 0;
};

class AvgAS final : public AggState {
   public:
    explicit AvgAS(const ColumnType type) : type_(type) {}

   public:
    void Consume(const std::optional<std::string_view> value) override {
        if (!value.has_value()) {
            throw Error::InvalidState("executor", "AVG requires a value");
        }
        sum_ += ParseToInt128(type_, *value);
        ++count_;
    }

    std::string Finalize() const override { return FormatAverage(sum_, count_); }

   private:
    ColumnType type_;
    Int128 sum_ = 0;
    uint64_t count_ = 0;
};

class ExtremumAS final : public AggState {
   public:
    ExtremumAS(const ColumnType type, const bool is_min) : type_(type), is_min_(is_min) {}

   public:
    void Consume(const std::optional<std::string_view> value) override {
        if (!value.has_value()) {
            throw Error::InvalidState("executor", "MIN/MAX requires a value");
        }

        if (!extremum_.has_value()) {
            extremum_ = std::string(*value);
            return;
        }

        if (ShouldReplaceExtremum(type_, *value, *extremum_, is_min_)) {
            extremum_ = std::string(*value);
        }
    }

    std::string Finalize() const override { return extremum_.value_or(""); }

   private:
    ColumnType type_;
    bool is_min_ = true;
    std::optional<std::string> extremum_;
};

class DistinctAS final : public AggState {
   public:
    explicit DistinctAS(std::unique_ptr<AggState> nested) : nested_(std::move(nested)) {}

   public:
    void Consume(const std::optional<std::string_view> value) override {
        if (!value.has_value()) {
            throw Error::InvalidState("executor", "DISTINCT requires a value");
        }

        if (seen_.insert(std::string(*value)).second) {
            nested_->Consume(value);
        }
    }

    std::string Finalize() const override { return nested_->Finalize(); }

   private:
    std::unique_ptr<AggState> nested_;
    std::unordered_set<std::string> seen_;
};

static bool SupportsAnyType(const ColumnType type) {
    (void)type;
    return true;
}

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

static std::unique_ptr<AggState> CreateCountState(const PlannedAgg& aggregate) {
    (void)aggregate;
    return std::make_unique<CountAS>();
}

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
    .distinct = true,
    .star = true,
    .supports_type = &SupportsAnyType,
    .factory = &CreateCountState,
});

[[maybe_unused]] static AggRegistrar register_sum({
    .canonical_name = "SUM",
    .distinct = false,
    .star = false,
    .supports_type = &SupportsNumericType,
    .factory = &CreateSumState,
});

[[maybe_unused]] static AggRegistrar register_avg({
    .canonical_name = "AVG",
    .distinct = false,
    .star = false,
    .supports_type = &SupportsNumericType,
    .factory = &CreateAvgState,
});

[[maybe_unused]] static AggRegistrar register_min({
    .canonical_name = "MIN",
    .distinct = false,
    .star = false,
    .supports_type = &SupportsAnyType,
    .factory = &CreateMinState,
});

[[maybe_unused]] static AggRegistrar register_max({
    .canonical_name = "MAX",
    .distinct = false,
    .star = false,
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
    if (aggregate.distinct && !aggregate.function->distinct) {
        throw Error::Unsupported("executor", "DISTINCT is not supported for aggregate '" +
                                                 std::string(aggregate.function->canonical_name) + "'");
    }
    if (aggregate.argument_kind == AggArgumentKind::Star && !aggregate.function->star) {
        throw Error::Unsupported(
            "executor", "aggregate '" + std::string(aggregate.function->canonical_name) + "' does not support '*'");
    }
    std::unique_ptr<AggState> state = aggregate.function->factory(aggregate);
    if (aggregate.distinct) {
        state = std::make_unique<DistinctAS>(std::move(state));
    }
    return state;
}
