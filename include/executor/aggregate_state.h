#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "executor/query_plan.h"

class AggState {
   public:
    AggState() = default;
    AggState(const AggState&) = delete;
    AggState(AggState&&) noexcept = default;
    AggState& operator=(const AggState&) = delete;
    AggState& operator=(AggState&&) noexcept = default;
    virtual ~AggState() = default;

    virtual void ConsumeValue(std::string_view value) = 0;
    virtual void ConsumeRow() = 0;
    virtual std::string Finalize() const = 0;
};

std::unique_ptr<AggState> CreateAggState(const PlannedAgg& aggregate);
