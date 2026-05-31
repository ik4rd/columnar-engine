#include <algorithm>
#include <chrono>
#include <compare>
#include <cstddef>
#include <regex>
#include <span>
#include <utility>

#include "common/ascii.h"
#include "common/error.h"
#include "common/parsing.h"
#include "common/string_pattern_utils.h"
#include "executor/comparison_utils.h"
#include "executor/operators_internal.h"
#include "executor/query_utils.h"
#include "executor/typed_value_utils.h"

constexpr std::string_view CountStarName = "COUNT(*)";
constexpr std::string_view FallbackAggregateAlias = "c";
constexpr std::string_view PageViewsAlias = "PageViews";
constexpr std::string_view ExtractMinutePart = "MINUTE";
constexpr std::string_view ExtractHourPart = "HOUR";

constexpr int64_t MinuteMicros = 60'000'000;

constexpr std::string_view HttpScheme = "http://";
constexpr std::string_view HttpsScheme = "https://";
constexpr std::string_view WwwPrefix = "www.";
constexpr std::string_view RegexBackrefPlaceholder = "\\1";
constexpr std::string_view RegexBackrefReplacement = "$1";

Int128 EvalInt(const std::string& value) { return ParseInt128(value); }

bool IsAggregateLookupFunction(const std::string_view name) {
    return name == "COUNT" || name == "SUM" || name == "AVG" || name == "MIN" || name == "MAX";
}

std::string FormatAggregateExprName(const ExprSpec& expr) {
    const std::string name = ToUpperAscii(expr.function_name);
    if (name == "COUNT" && !expr.arguments.empty() && expr.arguments.front()->kind == ExprKind::Star) {
        return std::string(CountStarName);
    }

    std::string out = name + "(";

    for (size_t i = 0; i < expr.arguments.size(); ++i) {
        if (i > 0) {
            out += ", ";
        }
        out += expr.arguments[i]->output_name;
    }

    out += ")";

    return out;
}

std::optional<std::string> TryResolveAggregateValue(const ExprSpec& expr, const Batch& batch, const size_t row) {
    const std::string name = ToUpperAscii(expr.function_name);
    if (!IsAggregateLookupFunction(name)) {
        return std::nullopt;
    }

    const std::string formatted_name = FormatAggregateExprName(expr);
    for (const std::string_view candidate_name :
         {std::string_view{formatted_name}, FallbackAggregateAlias, PageViewsAlias}) {
        if (const auto column = TryFindBatchColumn(batch.GetSchema(), candidate_name); column.has_value()) {
            return batch.ColumnAt(*column).ValueAsString(row);
        }
    }

    return std::nullopt;
}

std::string EvalFunction(const ExprSpec& expr, const Batch& batch, const size_t row) {
    if (const auto aggregate_value = TryResolveAggregateValue(expr, batch, row); aggregate_value.has_value()) {
        return *aggregate_value;
    }

    const std::string name = ToUpperAscii(expr.function_name);

    if (name == "STRLEN" || name == "LENGTH") {
        return std::to_string(EvalExpr(expr.arguments.at(0), batch, row).size());
    }

    if (name == "EXTRACT") {
        const std::string part = ToUpperAscii(EvalExpr(expr.arguments.at(0), batch, row));
        const int64_t ts = ParseTimestamp(EvalExpr(expr.arguments.at(1), batch, row));

        const std::chrono::sys_time<std::chrono::microseconds> time{std::chrono::microseconds{ts}};
        const auto day = std::chrono::floor<std::chrono::days>(time);

        const std::chrono::hh_mm_ss tod{time - day};

        if (part == ExtractMinutePart) {
            return std::to_string(tod.minutes().count());
        }
        if (part == ExtractHourPart) {
            return std::to_string(tod.hours().count());
        }

        throw Error::Unsupported("executor", "unsupported EXTRACT part");
    }

    if (name == "DATE_TRUNC") {
        const std::string part = ToUpperAscii(EvalExpr(expr.arguments.at(0), batch, row));
        int64_t micros = ParseTimestamp(EvalExpr(expr.arguments.at(1), batch, row));

        if (part == ExtractMinutePart) {
            micros = micros / MinuteMicros * MinuteMicros;
            return TimestampToString(micros);
        }

        throw Error::Unsupported("executor", "unsupported DATE_TRUNC part");
    }

    if (name == "REGEXP_REPLACE") {
        const std::string source = EvalExpr(expr.arguments.at(0), batch, row);
        const std::string pattern_text = EvalExpr(expr.arguments.at(1), batch, row);
        std::string replacement = EvalExpr(expr.arguments.at(2), batch, row);

        if (pattern_text == R"(^https?://(?:www\.)?([^/]+)/.*$)" && replacement == R"(\1)") {
            std::string_view rest = source;
            if (rest.starts_with(HttpScheme)) {
                rest.remove_prefix(HttpScheme.size());
            } else if (rest.starts_with(HttpsScheme)) {
                rest.remove_prefix(HttpsScheme.size());
            } else {
                return source;
            }

            if (rest.starts_with(WwwPrefix)) {
                rest.remove_prefix(WwwPrefix.size());
            }

            const size_t slash = rest.find('/');
            if (slash == std::string_view::npos || slash == 0) {
                return source;
            }

            const std::string_view suffix = rest.substr(slash + 1);
            if (suffix.find('\n') != std::string_view::npos || suffix.find('\r') != std::string_view::npos) {
                return source;
            }

            return std::string(rest.substr(0, slash));
        }

        if (expr.regex_replace_bound) {
            return std::regex_replace(source, expr.regex_pattern, expr.regex_replacement);
        }

        for (size_t pos = 0; (pos = replacement.find(RegexBackrefPlaceholder, pos)) != std::string::npos;) {
            replacement.replace(pos, RegexBackrefPlaceholder.size(), RegexBackrefReplacement);
            pos += RegexBackrefReplacement.size();
        }

        const std::regex pattern(pattern_text);

        return std::regex_replace(source, pattern, replacement);
    }

    throw Error::Unsupported("executor", "unsupported function '" + expr.function_name + "'");
}

bool MatchesInt128Comparison(const Int128 lhs, const Int128 rhs, const ComparisonKind comparison) {
    switch (comparison) {
        case ComparisonKind::Equal:
            return lhs == rhs;
        case ComparisonKind::NotEqual:
            return lhs != rhs;
        case ComparisonKind::Less:
            return lhs < rhs;
        case ComparisonKind::LessOrEqual:
            return lhs <= rhs;
        case ComparisonKind::Greater:
            return lhs > rhs;
        case ComparisonKind::GreaterOrEqual:
            return lhs >= rhs;
    }

    return false;
}

ValueComparison ToValueComparison(const ComparisonKind comparison) {
    switch (comparison) {
        case ComparisonKind::Equal:
            return ValueComparison::Equal;
        case ComparisonKind::NotEqual:
            return ValueComparison::NotEqual;
        case ComparisonKind::Less:
            return ValueComparison::Less;
        case ComparisonKind::LessOrEqual:
            return ValueComparison::LessOrEqual;
        case ComparisonKind::Greater:
            return ValueComparison::Greater;
        case ComparisonKind::GreaterOrEqual:
            return ValueComparison::GreaterOrEqual;
    }

    return ValueComparison::Equal;
}

std::optional<bool> TryEvaluateTypedComparison(const ExprPtr& left, const ExprPtr& right, const Batch& batch,
                                               const size_t row, const ComparisonKind comparison) {
    if (!left || !right) {
        return std::nullopt;
    }

    if (left->kind != ExprKind::Column || right->kind != ExprKind::Literal) {
        if (left->kind == ExprKind::Literal && right->kind == ExprKind::Column) {
            return TryEvaluateTypedComparison(right, left, batch, row, FlipComparison(comparison));
        }
        return std::nullopt;
    }

    const auto column_index = TryFindBatchColumn(batch.GetSchema(), left->column.name);
    if (!column_index.has_value()) {
        return std::nullopt;
    }

    const Column& column = batch.ColumnAt(*column_index);
    if (column.Type() == ColumnType::String) {
        return std::nullopt;
    }

    const std::optional<Int128> rhs = TryParseLiteralValueAsInt128(right->literal, column.Type());
    if (!rhs.has_value()) {
        return std::nullopt;
    }

    return MatchesInt128Comparison(column.ValueAsInt128(row), *rhs, comparison);
}

ColumnType InferComparisonType(const std::string& lhs, const std::string& rhs) {
    if (TryParseInt128(lhs).has_value() && TryParseInt128(rhs).has_value()) {
        return ColumnType::Int128;
    }

    if (TryParseDate(lhs).has_value() && TryParseDate(rhs).has_value()) {
        return ColumnType::Date;
    }

    if (TryParseTimestamp(lhs).has_value() && TryParseTimestamp(rhs).has_value()) {
        return ColumnType::Timestamp;
    }

    return ColumnType::String;
}

struct RowRef {
    size_t batch_index = 0;
    size_t row = 0;

    size_t ordinal = 0;
};

std::strong_ordering CompareColumnRows(const Column& lhs_column, const size_t lhs_row, const Column& rhs_column,
                                       const size_t rhs_row, const ColumnType type) {
    if (type != ColumnType::String) {
        return lhs_column.ValueAsInt128(lhs_row) <=> rhs_column.ValueAsInt128(rhs_row);
    }

    return lhs_column.ValueAsString(lhs_row) <=> rhs_column.ValueAsString(rhs_row);
}

class RowOrdering {
   public:
    RowOrdering(std::vector<PlannedOrderBy> order_by, const std::vector<Batch>* batches)
        : order_by_(std::move(order_by)), batches_(batches) {}

    bool operator()(const RowRef& lhs, const RowRef& rhs) const {
        const Batch& lhs_batch = batches_->at(lhs.batch_index);
        const Batch& rhs_batch = batches_->at(rhs.batch_index);

        for (const auto& order : order_by_) {
            const Column& lhs_column = lhs_batch.ColumnAt(order.result_column_index);
            const Column& rhs_column = rhs_batch.ColumnAt(order.result_column_index);
            const std::strong_ordering comparison =
                CompareColumnRows(lhs_column, lhs.row, rhs_column, rhs.row, order.value_type);
            if (comparison != 0) {
                return order.descending ? comparison > 0 : comparison < 0;
            }
        }

        return lhs.ordinal < rhs.ordinal;
    }

   private:
    std::vector<PlannedOrderBy> order_by_;

    const std::vector<Batch>* batches_ = nullptr;
};

Batch BuildBatchFromRowRefs(const Schema& schema, const std::vector<Batch>& batches, const std::vector<RowRef>& rows) {
    Batch result(schema, rows.size());

    for (const auto& row : rows) {
        const Batch& source = batches[row.batch_index];
        for (size_t column = 0; column < source.ColumnsCount(); ++column) {
            result.AppendValueFromColumn(column, source.ColumnAt(column), row.row);
        }
    }

    return result;
}

class FilterOperator final : public Operator {
   public:
    FilterOperator(std::unique_ptr<Operator> child, PredicatePtr filter)
        : child_(std::move(child)), filter_(std::move(filter)) {}

    std::optional<Batch> Next() override {
        while (auto batch = child_->Next()) {
            std::vector<size_t> selected_rows;
            SelectRowsMatchingPredicate(filter_, *batch, selected_rows);

            if (selected_rows.empty()) {
                continue;
            }

            Batch filtered(batch->GetSchema(), selected_rows.size());
            filtered.AppendRowsSelectedFromBatch(*batch, selected_rows);

            return filtered;
        }

        return std::nullopt;
    }

   private:
    std::unique_ptr<Operator> child_;

    PredicatePtr filter_;
};

class EnsureSchemaOperator final : public Operator {
   public:
    EnsureSchemaOperator(std::unique_ptr<Operator> child, Schema schema)
        : child_(std::move(child)), schema_(std::move(schema)) {}

    std::optional<Batch> Next() override {
        if (returned_empty_) {
            return std::nullopt;
        }

        if (auto batch = child_->Next()) {
            returned_any_ = true;
            return batch;
        }

        if (returned_any_) {
            return std::nullopt;
        }

        returned_any_ = true;
        returned_empty_ = true;

        return Batch(schema_);
    }

   private:
    std::unique_ptr<Operator> child_;

    Schema schema_;

    bool returned_any_ = false;
    bool returned_empty_ = false;
};

class ProjectionOperator final : public Operator {
   public:
    ProjectionOperator(std::unique_ptr<Operator> child, std::vector<SelectItemSpec> items, Schema source_schema)
        : child_(std::move(child)),
          items_(std::move(items)),
          source_schema_(std::move(source_schema)),
          schema_(ProjectionOutputSchema(items_, source_schema_)) {}

    std::optional<Batch> Next() override {
        while (auto batch = child_->Next()) {
            if (!star_column_indexes_initialized_) {
                star_column_indexes_ = StarColumnIndexes(batch->GetSchema());
                star_column_indexes_initialized_ = true;
            }

            Batch projected(schema_, batch->RowsCount());

            size_t output_column = 0;
            for (const auto& item : items_) {
                if (item.expression && item.expression->kind == ExprKind::Star) {
                    for (const size_t source_column : star_column_indexes_) {
                        projected.AppendColumnRange(output_column++, batch->ColumnAt(source_column), 0,
                                                    batch->RowsCount());
                    }
                    continue;
                }

                const size_t expression_column = output_column++;
                for (size_t row = 0; row < batch->RowsCount(); ++row) {
                    projected.AppendValueFromString(expression_column, EvalExpr(item.expression, *batch, row));
                }
            }

            if (projected.RowsCount() > 0) {
                return projected;
            }
        }

        return std::nullopt;
    }

   private:
    std::unique_ptr<Operator> child_;

    std::vector<SelectItemSpec> items_;

    Schema source_schema_;
    Schema schema_;

    std::vector<size_t> star_column_indexes_;

    bool star_column_indexes_initialized_ = false;
};

class OrderByOperator final : public Operator {
   public:
    OrderByOperator(std::unique_ptr<Operator> child, std::vector<PlannedOrderBy> order_by)
        : child_(std::move(child)), ordering_(std::move(order_by), &batches_) {}

    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }

        returned_ = true;

        std::optional<Schema> schema;
        std::vector<RowRef> rows;
        size_t ordinal = 0;

        while (auto batch = child_->Next()) {
            if (!schema.has_value()) {
                schema = batch->GetSchema();
            }

            const size_t batch_index = batches_.size();
            const size_t rows_count = batch->RowsCount();
            batches_.push_back(std::move(*batch));

            for (size_t row = 0; row < rows_count; ++row) {
                rows.push_back(RowRef{
                    .batch_index = batch_index,
                    .row = row,
                    .ordinal = ordinal++,
                });
            }
        }

        if (!schema.has_value()) {
            return std::nullopt;
        }

        std::ranges::sort(rows, ordering_);

        return BuildBatchFromRowRefs(*schema, batches_, rows);
    }

   private:
    std::unique_ptr<Operator> child_;

    std::vector<Batch> batches_;
    RowOrdering ordering_;

    bool returned_ = false;
};

class TopKOperator final : public Operator {
   public:
    TopKOperator(std::unique_ptr<Operator> child, std::vector<PlannedOrderBy> order_by, const size_t limit)
        : child_(std::move(child)), ordering_(std::move(order_by), &batches_), limit_(limit) {}

    std::optional<Batch> Next() override {
        if (returned_ || limit_ == 0) {
            return std::nullopt;
        }

        returned_ = true;

        std::optional<Schema> schema;
        std::vector<RowRef> rows;
        rows.reserve(limit_);

        size_t ordinal = 0;

        while (auto batch = child_->Next()) {
            if (!schema.has_value()) {
                schema = batch->GetSchema();
            }

            const size_t batch_index = batches_.size();
            const size_t rows_count = batch->RowsCount();
            batches_.push_back(std::move(*batch));

            for (size_t row = 0; row < rows_count; ++row) {
                RowRef candidate{
                    .batch_index = batch_index,
                    .row = row,
                    .ordinal = ordinal++,
                };

                if (rows.size() < limit_) {
                    rows.push_back(std::move(candidate));
                    std::ranges::push_heap(rows, ordering_);
                    continue;
                }

                if (ordering_(candidate, rows.front())) {
                    std::ranges::pop_heap(rows, ordering_);
                    rows.back() = std::move(candidate);
                    std::ranges::push_heap(rows, ordering_);
                }
            }
        }

        if (!schema.has_value()) {
            return std::nullopt;
        }

        std::ranges::sort(rows, ordering_);

        return BuildBatchFromRowRefs(*schema, batches_, rows);
    }

   private:
    std::unique_ptr<Operator> child_;

    std::vector<Batch> batches_;
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
            limited.AppendRowsRangeFromBatch(*batch, 0, remaining_);
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

class OffsetOperator final : public Operator {
   public:
    OffsetOperator(std::unique_ptr<Operator> child, const size_t offset)
        : child_(std::move(child)), remaining_(offset) {}

    std::optional<Batch> Next() override {
        while (const auto batch = child_->Next()) {
            if (remaining_ >= batch->RowsCount()) {
                remaining_ -= batch->RowsCount();
                continue;
            }

            Batch shifted(batch->GetSchema(), batch->RowsCount() - remaining_);
            shifted.AppendRowsRangeFromBatch(*batch, remaining_, batch->RowsCount() - remaining_);
            remaining_ = 0;

            return shifted;
        }

        return std::nullopt;
    }

   private:
    std::unique_ptr<Operator> child_;

    size_t remaining_;
};

std::string EvalExpr(const ExprPtr& expr, const Batch& batch, const size_t row) {
    switch (expr->kind) {
        case ExprKind::Column: {
            if (expr->column_index_bound && expr->column_index < batch.ColumnsCount() &&
                SameColumnName(batch.GetSchema().columns[expr->column_index].name, expr->column.name)) {
                return batch.ColumnAt(expr->column_index).ValueAsString(row);
            }

            const auto column = TryFindBatchColumn(batch.GetSchema(), expr->column.name);
            if (!column.has_value()) {
                throw Error::InvalidArgument("executor", "unknown column '" + expr->column.name + "'");
            }

            return batch.ColumnAt(*column).ValueAsString(row);
        }
        case ExprKind::Literal:
            return NormalizeLiteralForEval(expr->literal);
        case ExprKind::Binary: {
            const Int128 lhs = EvalInt(EvalExpr(expr->left, batch, row));
            const Int128 rhs = EvalInt(EvalExpr(expr->right, batch, row));
            return Int128ToString(expr->binary_op == BinaryOp::Add ? lhs + rhs : lhs - rhs);
        }
        case ExprKind::Function:
            return EvalFunction(*expr, batch, row);
        case ExprKind::Case:
            return EvaluatePredicate(expr->case_spec.condition, batch, row)
                       ? EvalExpr(expr->case_spec.then_expr, batch, row)
                       : EvalExpr(expr->case_spec.else_expr, batch, row);
        case ExprKind::Star:
            return "*";
    }
    return {};
}

bool EvaluatePredicate(const PredicatePtr& predicate, const Batch& batch, const size_t row) {
    if (!predicate) {
        return true;
    }

    switch (predicate->kind) {
        case PredicateKind::And:
            return EvaluatePredicate(predicate->lhs, batch, row) && EvaluatePredicate(predicate->rhs, batch, row);
        case PredicateKind::Like:
            return LikeMatches(EvalExpr(predicate->left, batch, row), EvalExpr(predicate->right, batch, row));
        case PredicateKind::NotLike:
            return !LikeMatches(EvalExpr(predicate->left, batch, row), EvalExpr(predicate->right, batch, row));
        case PredicateKind::In: {
            if (predicate->literal_in_set_bound && predicate->in_column_index < batch.ColumnsCount()) {
                return predicate->literal_in_values.contains(
                    batch.ColumnAt(predicate->in_column_index).ValueAsString(row));
            }

            const std::string lhs = EvalExpr(predicate->left, batch, row);

            return std::ranges::any_of(predicate->values,
                                       [&](const ExprPtr& value) { return lhs == EvalExpr(value, batch, row); });
        }
        case PredicateKind::Comparison: {
            if (predicate->typed_literal_comparison_bound && predicate->typed_column_index < batch.ColumnsCount()) {
                return MatchesInt128Comparison(batch.ColumnAt(predicate->typed_column_index).ValueAsInt128(row),
                                               predicate->typed_literal_value, predicate->typed_comparison);
            }

            if (const std::optional<bool> result =
                    TryEvaluateTypedComparison(predicate->left, predicate->right, batch, row, predicate->comparison);
                result.has_value()) {
                return *result;
            }

            const std::string lhs = EvalExpr(predicate->left, batch, row);
            const std::string rhs = EvalExpr(predicate->right, batch, row);

            return MatchesComparison(InferComparisonType(lhs, rhs), lhs, rhs, predicate->comparison);
        }
    }

    return false;
}

void SelectRowsMatchingPredicate(const PredicatePtr& predicate, const Batch& batch, std::vector<size_t>& rows) {
    rows.clear();
    rows.reserve(batch.RowsCount());

    if (!predicate) {
        for (size_t row = 0; row < batch.RowsCount(); ++row) {
            rows.push_back(row);
        }
        return;
    }

    if (predicate->kind == PredicateKind::Comparison && predicate->typed_literal_comparison_bound &&
        predicate->typed_column_index < batch.ColumnsCount()) {
        batch.ColumnAt(predicate->typed_column_index)
            .SelectRowsByInt128Comparison(predicate->typed_literal_value,
                                          ToValueComparison(predicate->typed_comparison), rows);
        return;
    }

    if ((predicate->kind == PredicateKind::Like || predicate->kind == PredicateKind::NotLike) &&
        predicate->literal_like_pattern_bound && predicate->like_column_index < batch.ColumnsCount()) {
        batch.ColumnAt(predicate->like_column_index)
            .SelectRowsByLikePattern(predicate->like_pattern, predicate->like_negated, rows);
        return;
    }

    if (predicate->kind == PredicateKind::In && predicate->literal_in_set_bound &&
        predicate->in_column_index < batch.ColumnsCount()) {
        batch.ColumnAt(predicate->in_column_index).SelectRowsByStringSet(predicate->literal_in_values, rows);
        return;
    }

    for (size_t row = 0; row < batch.RowsCount(); ++row) {
        if (EvaluatePredicate(predicate, batch, row)) {
            rows.push_back(row);
        }
    }
}

Schema BuildSelectOutputSchema(const std::vector<PlannedSelectItem>& select_items) {
    Schema schema;
    schema.columns.reserve(select_items.size());

    for (const auto& item : select_items) {
        schema.columns.push_back(ColumnSchema(item.output_name, item.output_type));
    }

    return schema;
}

Schema ProjectionOutputSchema(const std::vector<SelectItemSpec>& items, const Schema& source_schema) {
    Schema schema;

    for (const auto& item : items) {
        if (item.expression && item.expression->kind == ExprKind::Star) {
            for (const size_t source_index : StarColumnIndexes(source_schema)) {
                schema.columns.push_back(source_schema.columns[source_index]);
            }
        } else {
            schema.columns.emplace_back(item.output_name, ColumnType::String);
        }
    }

    return schema;
}

std::unique_ptr<Operator> CreateFilterOperator(std::unique_ptr<Operator> child, PredicatePtr filter) {
    return std::make_unique<FilterOperator>(std::move(child), std::move(filter));
}

std::unique_ptr<Operator> CreateEnsureSchemaOperator(std::unique_ptr<Operator> child, Schema schema) {
    return std::make_unique<EnsureSchemaOperator>(std::move(child), std::move(schema));
}

std::unique_ptr<Operator> CreateProjectionOperator(std::unique_ptr<Operator> child, std::vector<SelectItemSpec> items,
                                                   Schema source_schema) {
    return std::make_unique<ProjectionOperator>(std::move(child), std::move(items), std::move(source_schema));
}

void ApplyOrderOffsetLimit(std::unique_ptr<Operator>& root, const PlannedQuery& planned, bool& limit_applied_by_top_k) {
    if (!planned.order_by.empty()) {
        if (planned.limit.has_value() && planned.offset == 0) {
            root = std::make_unique<TopKOperator>(std::move(root), planned.order_by, *planned.limit);
            limit_applied_by_top_k = true;
        } else {
            root = std::make_unique<OrderByOperator>(std::move(root), planned.order_by);
        }
    }

    if (planned.offset > 0) {
        root = std::make_unique<OffsetOperator>(std::move(root), planned.offset);
    }

    if (planned.limit.has_value() && !limit_applied_by_top_k) {
        root = std::make_unique<LimitOperator>(std::move(root), *planned.limit);
    }
}
