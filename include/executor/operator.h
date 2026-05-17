#pragma once

#include <memory>
#include <optional>

#include "executor/query_plan.h"
#include "model/batch.h"

class Operator {
   public:
    Operator() = default;
    Operator(const Operator&) = delete;
    Operator(Operator&&) noexcept = default;
    Operator& operator=(const Operator&) = delete;
    Operator& operator=(Operator&&) noexcept = default;
    virtual ~Operator() = default;

    virtual std::optional<Batch> Next() = 0;
};

std::unique_ptr<Operator> BuildPlan(const PlannedQuery& planned);
