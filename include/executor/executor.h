#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "executor/aggregate_function.h"
#include "model/batch.h"
#include "support/error.h"
#include "tl/expected.hpp"
#include "sql_parser/tokenizer.h"

using ExecuteExpected = tl::expected<Batch, Error>;

class Operator {
   public:
    Operator() = default;
    Operator(const Operator&) = delete;
    Operator(Operator&&) noexcept = default;
    Operator& operator=(const Operator&) = delete;
    Operator& operator=(Operator&&) noexcept = default;
    virtual ~Operator() = default;

   public:
    virtual std::optional<Batch> Next() = 0;
};

class Executor {
   public:
    Executor() = default;

   public:
    void RegisterTable(const std::string& name, std::filesystem::path path);
    ExecuteExpected Execute(std::string_view query) const;

   private:
    std::unordered_map<std::string, std::filesystem::path> tables_;
};

enum class ComparisonKind {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
};

struct ParsedLiteral {
    std::string text;
    Tokens token_type = Tokens::StringLiteral;
};

struct FilterSpec {
    std::string column_name;
    ComparisonKind comparison = ComparisonKind::Equal;
    ParsedLiteral literal;
};

struct AggSpec {
    const AggFuncDefinition* function = nullptr;
    bool distinct = false;
    bool star = false;
    std::string column_name;
    std::string output_name;
};

struct ParsedQuery {
    std::string table_name;
    std::vector<AggSpec> aggregates;
    std::optional<FilterSpec> filter;
};

struct PlannedAgg {
    const AggFuncDefinition* function = nullptr;
    bool distinct = false;
    bool star = false;
    std::optional<size_t> column_index;
    ColumnType input_type = ColumnType::String;
    std::string output_name;
};

struct PlannedFilter {
    size_t column_index = 0;
    ColumnType column_type = ColumnType::String;
    ComparisonKind comparison = ComparisonKind::Equal;
    std::string literal_text;
};

struct PlannedQuery {
    std::filesystem::path table_path;
    std::vector<size_t> projection_indexes;
    std::optional<PlannedFilter> filter;
    std::vector<PlannedAgg> aggregates;
};

class AggState {
   public:
    AggState() = default;
    AggState(const AggState&) = delete;
    AggState(AggState&&) noexcept = default;
    AggState& operator=(const AggState&) = delete;
    AggState& operator=(AggState&&) noexcept = default;
    virtual ~AggState() = default;

   public:
    virtual void Consume(std::optional<std::string_view> value) = 0;
    virtual std::string Finalize() const = 0;
};

ParsedQuery ParseQuery(std::string_view query);
PlannedQuery PlanQuery(const ParsedQuery& parsed, const std::unordered_map<std::string, std::filesystem::path>& tables);
std::unique_ptr<AggState> CreateAggState(const PlannedAgg& aggregate);
std::unique_ptr<Operator> BuildPlan(const PlannedQuery& planned);
