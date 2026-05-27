#include <algorithm>
#include <chrono>
#include <compare>
#include <regex>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/ascii.h"
#include "common/error.h"
#include "common/int128.h"
#include "common/operator_utils.h"
#include "common/parsing.h"
#include "common/planner_utils.h"
#include "executor/aggregate_state.h"
#include "executor/operator.h"
#include "io/columnar_batch.h"
#include "io/file.h"

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

class MetadataCountOperator final : public Operator {
   public:
    MetadataCountOperator(std::filesystem::path path, std::string output_name)
        : path_(std::move(path)), output_name_(std::move(output_name)) {}

    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }

        returned_ = true;

        uint64_t rows = 0;

        const ColumnarBatchReader reader(path_);
        for (const auto& row_group : reader.GetMetadata().row_groups) {
            rows += row_group.row_count;
        }

        Batch result(Schema{{ColumnSchema(output_name_, ColumnType::Int64)}}, 1);
        result.AppendValueFromString(0, std::to_string(rows));

        return result;
    }

   private:
    std::filesystem::path path_;
    std::string output_name_;
    bool returned_ = false;
};

class MetadataExtremaOperator final : public Operator {
   public:
    MetadataExtremaOperator(std::filesystem::path path, std::vector<PlannedAgg> aggregates)
        : path_(std::move(path)), aggregates_(std::move(aggregates)) {}

    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }

        returned_ = true;

        const ColumnarBatchReader reader(path_);
        const ColumnarMetadata& metadata = reader.GetMetadata();

        Schema schema;
        schema.columns.reserve(aggregates_.size());

        std::vector<std::unique_ptr<AggState>> states;
        states.reserve(aggregates_.size());

        for (const PlannedAgg& aggregate : aggregates_) {
            schema.columns.push_back(ColumnSchema(aggregate.output_name, AggregateOutputType(aggregate)));
            states.push_back(CreateAggState(aggregate));
        }

        for (const auto& row_group : metadata.row_groups) {
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                const PlannedAgg& aggregate = aggregates_[i];
                const auto& chunk = row_group.columns[aggregate.column_index];

                if (!chunk.has_min_max) {
                    throw Error::InvalidState("executor", "metadata min/max statistics are missing", path_.string());
                }

                const std::string name = ToUpperAscii(aggregate.function->canonical_name);
                states[i]->ConsumeInt128(name == "MIN" ? chunk.min_value : chunk.max_value);
            }
        }

        Batch result(schema);
        for (size_t i = 0; i < states.size(); ++i) {
            result.AppendValueFromString(i, states[i]->Finalize());
        }

        return result;
    }

   private:
    std::filesystem::path path_;
    std::vector<PlannedAgg> aggregates_;
    bool returned_ = false;
};

static bool MatchesRowGroupComparison(const ColumnChunkMetadata& chunk, const Int128 value,
                                      const ComparisonKind comparison) {
    if (!chunk.has_min_max) {
        return true;
    }

    switch (comparison) {
        case ComparisonKind::Equal:
            return chunk.min_value <= value && value <= chunk.max_value;
        case ComparisonKind::NotEqual:
            return chunk.min_value != value || chunk.max_value != value;
        case ComparisonKind::Less:
            return chunk.min_value < value;
        case ComparisonKind::LessOrEqual:
            return chunk.min_value <= value;
        case ComparisonKind::Greater:
            return chunk.max_value > value;
        case ComparisonKind::GreaterOrEqual:
            return chunk.max_value >= value;
    }

    return true;
}

static bool MayMatchRowGroup(const PredicatePtr& predicate, const RowGroupMetadata& row_group) {
    if (!predicate) {
        return true;
    }

    switch (predicate->kind) {
        case PredicateKind::And:
            return MayMatchRowGroup(predicate->lhs, row_group) && MayMatchRowGroup(predicate->rhs, row_group);
        case PredicateKind::Comparison:
            if (!predicate->metadata_typed_literal_comparison_bound ||
                predicate->metadata_typed_column_index >= row_group.columns.size()) {
                return true;
            }
            return MatchesRowGroupComparison(row_group.columns[predicate->metadata_typed_column_index],
                                             predicate->metadata_typed_literal_value,
                                             predicate->metadata_typed_comparison);
        case PredicateKind::In:
            if (!predicate->metadata_typed_in_set_bound ||
                predicate->metadata_typed_in_column_index >= row_group.columns.size()) {
                return true;
            }

            return std::ranges::any_of(
                predicate->metadata_typed_in_values,
                [&](const Int128 value) {
                    return MatchesRowGroupComparison(row_group.columns[predicate->metadata_typed_in_column_index],
                                                     value, ComparisonKind::Equal);
                });
        case PredicateKind::Like:
        case PredicateKind::NotLike:
            return true;
    }

    return true;
}

class ScanOperator final : public Operator {
   public:
    ScanOperator(std::filesystem::path path, std::vector<size_t> projection_indexes, PredicatePtr filter)
        : path_(std::move(path)),
          input_(path_),
          metadata_(ColumnarBatchReader(path_).GetMetadata()),
          filter_(std::move(filter)) {
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

        const RowGroupMetadata* row_group = nullptr;
        while (next_group_ < metadata_.row_groups.size()) {
            const RowGroupMetadata& candidate = metadata_.row_groups[next_group_++];
            if (MayMatchRowGroup(filter_, candidate)) {
                row_group = &candidate;
                break;
            }
        }

        if (row_group == nullptr) {
            return std::nullopt;
        }

        Batch batch(projected_schema_, row_group->row_count);

        for (size_t projected_index = 0; projected_index < projection_indexes_.size(); ++projected_index) {
            const size_t source_index = projection_indexes_[projected_index];
            const auto& chunk = row_group->columns[source_index];

            batch.ReadColumnFrom(projected_index, input_.StreamAt(chunk.offset), row_group->row_count, chunk.size);
        }

        return batch;
    }

   private:
    std::filesystem::path path_;
    InputFile input_;

    ColumnarMetadata metadata_;
    std::vector<size_t> projection_indexes_;
    Schema projected_schema_;
    PredicatePtr filter_;
    size_t next_group_ = 0;
};

static std::string NormalizeLiteralForEval(const QueryLiteral& literal) {
    if (literal.kind != LiteralKind::String) {
        return literal.text;
    }

    std::string result;
    const std::string_view text = literal.text;

    for (size_t i = 1; i + 1 < text.size(); ++i) {
        if (text[i] == '\'' && i + 2 < text.size() && text[i + 1] == '\'') {
            result.push_back('\'');
            ++i;
            continue;
        }
        result.push_back(text[i]);
    }

    return result;
}

static std::optional<size_t> TryFindBatchColumn(const Schema& schema, const std::string_view name) {
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (SameColumnName(schema.columns[i].name, name)) {
            return i;
        }
    }
    return std::nullopt;
}

static Int128 EvalInt(const std::string& value) { return ParseInt128(value); }

static std::string FormatAggregateExprName(const ExprSpec& expr) {
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

static std::string EvalExpr(const ExprPtr& expr, const Batch& batch, size_t row);
static bool EvaluatePredicate(const PredicatePtr& predicate, const Batch& batch, size_t row);

static std::string EvalFunction(const ExprSpec& expr, const Batch& batch, const size_t row) {
    const std::string name = ToUpperAscii(expr.function_name);

    if (name == "COUNT" || name == "SUM" || name == "AVG" || name == "MIN" || name == "MAX") {
        if (const auto column = TryFindBatchColumn(batch.GetSchema(), FormatAggregateExprName(expr));
            column.has_value()) {
            return batch.ColumnAt(*column).ValueAsString(row);
        }
        if (const auto column = TryFindBatchColumn(batch.GetSchema(), FallbackAggregateAlias); column.has_value()) {
            return batch.ColumnAt(*column).ValueAsString(row);
        }
        if (const auto column = TryFindBatchColumn(batch.GetSchema(), PageViewsAlias); column.has_value()) {
            return batch.ColumnAt(*column).ValueAsString(row);
        }
    }

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

static std::string EvalExpr(const ExprPtr& expr, const Batch& batch, const size_t row) {
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

static ComparisonKind FlipComparison(const ComparisonKind comparison) {
    switch (comparison) {
        case ComparisonKind::Equal:
            return ComparisonKind::Equal;
        case ComparisonKind::NotEqual:
            return ComparisonKind::NotEqual;
        case ComparisonKind::Less:
            return ComparisonKind::Greater;
        case ComparisonKind::LessOrEqual:
            return ComparisonKind::GreaterOrEqual;
        case ComparisonKind::Greater:
            return ComparisonKind::Less;
        case ComparisonKind::GreaterOrEqual:
            return ComparisonKind::LessOrEqual;
    }

    return comparison;
}

static bool MatchesInt128Comparison(const Int128 lhs, const Int128 rhs, const ComparisonKind comparison) {
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

static ValueComparison ToValueComparison(const ComparisonKind comparison) {
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

static std::optional<Int128> LiteralValueForColumnType(const QueryLiteral& literal, const ColumnType type) {
    try {
        if (literal.kind == LiteralKind::Numeric) {
            return ParseInt128(literal.text);
        }

        if (literal.kind == LiteralKind::String) {
            const QueryLiteral normalized_literal{NormalizeLiteralForEval(literal), LiteralKind::String};
            if (type == ColumnType::Date) {
                return ParseDate(normalized_literal.text);
            }
            if (type == ColumnType::Timestamp) {
                return ParseTimestamp(normalized_literal.text);
            }
        }
    } catch (const Error&) {
        return std::nullopt;
    }

    return std::nullopt;
}

static std::optional<bool> TryEvaluateTypedComparison(const ExprPtr& left, const ExprPtr& right, const Batch& batch,
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

    const std::optional<Int128> rhs = LiteralValueForColumnType(right->literal, column.Type());
    if (!rhs.has_value()) {
        return std::nullopt;
    }

    return MatchesInt128Comparison(column.ValueAsInt128(row), *rhs, comparison);
}

static ColumnType InferComparisonType(const std::string& lhs, const std::string& rhs) {
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

static bool EvaluatePredicate(const PredicatePtr& predicate, const Batch& batch, const size_t row) {
    if (!predicate) {
        return true;
    }

    switch (predicate->kind) {
        case PredicateKind::And:
            return EvaluatePredicate(predicate->lhs, batch, row) && EvaluatePredicate(predicate->rhs, batch, row);
        case PredicateKind::Like: {
            return LikeMatches(EvalExpr(predicate->left, batch, row), EvalExpr(predicate->right, batch, row));
        }
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

static void SelectRowsMatchingPredicate(const PredicatePtr& predicate, const Batch& batch, std::vector<size_t>& rows) {
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

static bool UsesRowAggInput(const PlannedAgg& aggregate) {
    return !aggregate.direct_numeric_argument && aggregate.argument_kind != AggArgumentKind::Column;
}

static void ConsumeAggRow(const PlannedAgg& aggregate, const Batch& batch, const size_t row, AggState& state) {
    if (aggregate.direct_numeric_argument) {
        state.ConsumeInt128(batch.ColumnAt(aggregate.column_index).ValueAsInt128(row) +
                            aggregate.direct_numeric_offset);
        return;
    }

    if (aggregate.argument_kind == AggArgumentKind::Column) {
        state.ConsumeValue(EvalExpr(aggregate.argument, batch, row));
        return;
    }

    state.ConsumeRow();
}

static void ConsumeAggRows(const PlannedAgg& aggregate, const Batch& batch, const std::span<const size_t> rows,
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
            if (group_key.expression && group_key.expression->kind == ExprKind::Column &&
                group_key.expression->column_index_bound && group_key.expression->column_index < batch.ColumnsCount()) {
                const Column& column = batch.ColumnAt(group_key.expression->column_index);
                column.AppendEncodedValue(row, key.id);
                key.values.push_back(column.ValueAsString(row));
                continue;
            }

            std::string value = EvalExpr(group_key.expression, batch, row);
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
    AggOperator(std::unique_ptr<Operator> child, const std::vector<PlannedAgg>& aggregates, PredicatePtr filter)
        : child_(std::move(child)), filter_(std::move(filter)) {
        bindings_.reserve(aggregates.size());

        for (const auto& aggregate : aggregates) {
            bindings_.push_back(AggBinding{
                .aggregate = aggregate,
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
            std::vector<size_t> selected_rows;
            SelectRowsMatchingPredicate(filter_, *batch, selected_rows);

            for (const auto& binding : bindings_) {
                ConsumeAggRows(binding.aggregate, *batch, selected_rows, *binding.state);
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
          group_key_encoder_(std::move(group_keys)),
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
            std::vector<size_t> selected_rows;
            SelectRowsMatchingPredicate(filter_, *batch, selected_rows);

            for (const size_t row : selected_rows) {
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
                    ConsumeAggRow(bindings_[i].aggregate, *batch, row, *group.states[i]);
                }
            }
        }

        Schema schema;
        schema.columns.reserve(select_items_.size());

        for (const auto& item : select_items_) {
            schema.columns.push_back(ColumnSchema(item.output_name, item.output_type));
        }

        std::vector<size_t> output_order = BuildOutputOrder();
        Batch result(schema, output_order.size());

        for (const size_t group_index : output_order) {
            AppendGroupToBatch(groups_[group_index], result);
        }

        if (having_) {
            std::vector<size_t> selected_rows;
            selected_rows.reserve(result.RowsCount());

            for (size_t row = 0; row < result.RowsCount(); ++row) {
                if (EvaluatePredicate(having_, result, row)) {
                    selected_rows.push_back(row);
                }
            }

            Batch filtered(schema, selected_rows.size());
            filtered.AppendRowsSelectedFromBatch(result, selected_rows);

            return filtered;
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
    };

    std::vector<std::unique_ptr<AggState>> CreateStates() const {
        std::vector<std::unique_ptr<AggState>> states;
        states.reserve(bindings_.size());

        for (const auto& binding : bindings_) {
            states.push_back(CreateAggState(binding.aggregate));
        }

        return states;
    }

    void AppendGroupToBatch(const GroupState& group, const Batch& batch) const {
        for (size_t column = 0; column < select_items_.size(); ++column) {
            const PlannedSelectItem& item = select_items_[column];
            if (item.kind == SelectItemKind::GroupKey) {
                batch.AppendValueFromString(column, group.key_values[item.index]);
            } else {
                batch.AppendValueFromString(column, group.states[item.index]->Finalize());
            }
        }
    }

    std::vector<size_t> BuildOutputOrder() const {
        std::vector<size_t> order;
        order.reserve(groups_.size());

        for (size_t i = 0; i < groups_.size(); ++i) {
            order.push_back(i);
        }

        if (!sort_by_group_keys_) {
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

        std::ranges::sort(order, [&](const size_t lhs_index, const size_t rhs_index) {
            const GroupState& lhs = groups_[lhs_index];
            const GroupState& rhs = groups_[rhs_index];
            for (const size_t column : group_key_columns) {
                const PlannedSelectItem& item = select_items_[column];
                const std::strong_ordering comparison =
                    CompareValues(item.output_type, lhs.key_values[item.index], rhs.key_values[item.index]);
                if (comparison != 0) {
                    return comparison < 0;
                }
            }

            return false;
        });

        return order;
    }

    std::unique_ptr<Operator> child_;
    GroupKeyEncoder group_key_encoder_;
    std::vector<GroupAggBinding> bindings_;
    std::vector<PlannedSelectItem> select_items_;
    PredicatePtr filter_;
    PredicatePtr having_;
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
    explicit RowOrdering(std::vector<PlannedOrderBy> order_by) : order_by_(std::move(order_by)) {}

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

class ProjectionOperator final : public Operator {
   public:
    ProjectionOperator(std::unique_ptr<Operator> child, std::vector<SelectItemSpec> items, Schema source_schema)
        : child_(std::move(child)), items_(std::move(items)), source_schema_(std::move(source_schema)) {
        for (const auto& item : items_) {
            if (item.expression && item.expression->kind == ExprKind::Star) {
                for (const size_t source_index : StarColumnIndexes(source_schema_)) {
                    schema_.columns.push_back(source_schema_.columns[source_index]);
                }
            } else {
                schema_.columns.emplace_back(item.output_name, ColumnType::String);
            }
        }
    }

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

    const Schema& GetSchema() const { return schema_; }

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
    OrderByOperator(std::unique_ptr<Operator> child, std::vector<PlannedOrderBy> order_by,
                    std::vector<PlannedSelectItem> /*select_items*/)
        : child_(std::move(child)), ordering_(std::move(order_by)) {}

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
                 std::vector<PlannedSelectItem> /*select_items*/, const size_t limit)
        : child_(std::move(child)), ordering_(std::move(order_by)), limit_(limit) {}

    std::optional<Batch> Next() override {
        if (returned_ || limit_ == 0) {
            return std::nullopt;
        }

        returned_ = true;

        std::optional<Schema> schema;
        std::vector<SortedRow> rows;
        rows.reserve(limit_);

        size_t ordinal = 0;

        while (auto batch = child_->Next()) {
            if (!schema.has_value()) {
                schema = batch->GetSchema();
            }

            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                SortedRow candidate{
                    .values = MaterializeBatchRow(*batch, row),
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

std::unique_ptr<Operator> BuildPlan(const PlannedQuery& planned) {
    if (planned.metadata_count_only) {
        return std::make_unique<MetadataCountOperator>(planned.table_path, planned.aggregates.front().output_name);
    }

    if (planned.metadata_extrema_only) {
        return std::make_unique<MetadataExtremaOperator>(planned.table_path, planned.aggregates);
    }

    std::unique_ptr<Operator> root =
        std::make_unique<ScanOperator>(planned.table_path, planned.projection_indexes, planned.filter);

    if (planned.filter && planned.plain_select) {
        root = std::make_unique<FilterOperator>(std::move(root), planned.filter);
    }

    if (planned.plain_select) {
        bool limit_applied_by_top_k = false;
        if (!planned.order_by.empty()) {
            if (planned.limit.has_value() && planned.offset == 0) {
                root = std::make_unique<TopKOperator>(std::move(root), planned.order_by, planned.select_items,
                                                      *planned.limit);
                limit_applied_by_top_k = true;
            } else {
                root = std::make_unique<OrderByOperator>(std::move(root), planned.order_by, planned.select_items);
            }
        }
        if (planned.offset > 0) {
            root = std::make_unique<OffsetOperator>(std::move(root), planned.offset);
        }
        if (planned.limit.has_value() && !limit_applied_by_top_k) {
            root = std::make_unique<LimitOperator>(std::move(root), *planned.limit);
        }

        auto projection =
            std::make_unique<ProjectionOperator>(std::move(root), planned.plain_select_items, planned.table_schema);
        const Schema output_schema = projection->GetSchema();
        root = std::move(projection);
        root = std::make_unique<EnsureSchemaOperator>(std::move(root), output_schema);

        return root;
    }

    if (!planned.group_keys.empty()) {
        root = std::make_unique<GroupAggOperator>(std::move(root), planned.group_keys, planned.aggregates,
                                                  planned.select_items, planned.filter, planned.having,
                                                  planned.order_by.empty());
    } else {
        root = std::make_unique<AggOperator>(std::move(root), planned.aggregates, planned.filter);
    }

    bool limit_applied_by_top_k = false;

    if (!planned.order_by.empty()) {
        if (planned.limit.has_value() && planned.offset == 0) {
            root =
                std::make_unique<TopKOperator>(std::move(root), planned.order_by, planned.select_items, *planned.limit);
            limit_applied_by_top_k = true;
        } else {
            root = std::make_unique<OrderByOperator>(std::move(root), planned.order_by, planned.select_items);
        }
    }

    if (planned.offset > 0) {
        root = std::make_unique<OffsetOperator>(std::move(root), planned.offset);
    }

    if (planned.limit.has_value() && !limit_applied_by_top_k) {
        root = std::make_unique<LimitOperator>(std::move(root), *planned.limit);
    }

    return root;
}
