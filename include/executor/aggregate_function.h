#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "model/schema.h"

class AggState;
struct PlannedAgg;

using AggFactory = std::unique_ptr<AggState> (*)(const PlannedAgg&);
using AggTypeSupport = bool (*)(ColumnType);

enum class AggCallFeature : std::uint8_t {
    None = 0,
    Distinct = 1,
    Star = 2,
};

constexpr AggCallFeature operator|(const AggCallFeature lhs, const AggCallFeature rhs) {
    return static_cast<AggCallFeature>(static_cast<uint8_t>(lhs) | static_cast<uint8_t>(rhs));
}

constexpr bool HasAggCallFeature(const AggCallFeature features, const AggCallFeature feature) {
    return (static_cast<uint8_t>(features) & static_cast<uint8_t>(feature));
}

struct AggFuncDefinition {
    std::string_view canonical_name;
    AggCallFeature call_features = AggCallFeature::None;
    AggTypeSupport supports_type = nullptr;
    AggFactory factory = nullptr;
};

class AggRegistry {
   public:
    using RegistryMap = std::unordered_map<std::string, AggFuncDefinition>;

    static AggRegistry& Instance();

    void Register(const AggFuncDefinition& def);
    const AggFuncDefinition* Find(std::string_view name) const;

   private:
    RegistryMap registry_;
};

struct AggRegistrar {
    explicit AggRegistrar(const AggFuncDefinition& def);
};

const AggFuncDefinition& ResolveAggFunc(std::string_view name);
bool AggSupportsInputType(const AggFuncDefinition& definition, ColumnType type);
