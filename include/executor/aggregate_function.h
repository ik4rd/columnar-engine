#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "model/schema.h"

class AggState;
struct PlannedAgg;

using AggFactory = std::unique_ptr<AggState> (*)(const PlannedAgg&);
using AggTypeSupport = bool (*)(ColumnType);

struct AggFuncDefinition {
    std::string_view canonical_name;
    bool distinct = false;
    bool star = false;
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
