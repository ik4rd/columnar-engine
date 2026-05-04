#include <fstream>
#include <utility>
#include <vector>

#include "io/columnar_batch_io.h"
#include "executor/executor.h"
#include "io/fileio.h"
#include "support/parsing.h"

static void SeekRead(const std::filesystem::path& path, std::ifstream& in, const uint64_t offset) {
    in.seekg(static_cast<std::streamoff>(offset));
    if (!in) {
        throw Error::PathIo("operators", path, "seek file");
    }
}

template <typename T>
static bool EvaluateComparison(const T left, const T right, const ComparisonKind kind) {
    if (kind == ComparisonKind::Equal) {
        return left == right;
    }
    if (kind == ComparisonKind::NotEqual) {
        return left != right;
    }
    if (kind == ComparisonKind::Less) {
        return left < right;
    }
    if (kind == ComparisonKind::LessOrEqual) {
        return left <= right;
    }
    if (kind == ComparisonKind::Greater) {
        return left > right;
    }
    if (kind == ComparisonKind::GreaterOrEqual) {
        return left >= right;
    }
    throw Error::Unsupported("operators", "unsupported comparison kind");
}

static bool MatchesComparison(const ColumnType type, const std::string_view lhs, const std::string_view rhs,
                              const ComparisonKind kind) {
    switch (type) {
        case ColumnType::Boolean:
            return EvaluateComparison(ParseBoolean(std::string(lhs)), ParseBoolean(std::string(rhs)), kind);
        case ColumnType::Int16:
            return EvaluateComparison(ParseInt16(std::string(lhs)), ParseInt16(std::string(rhs)), kind);
        case ColumnType::Int32:
            return EvaluateComparison(ParseInt32(std::string(lhs)), ParseInt32(std::string(rhs)), kind);
        case ColumnType::Int64:
            return EvaluateComparison(ParseInt64(std::string(lhs)), ParseInt64(std::string(rhs)), kind);
        case ColumnType::Int128:
            return EvaluateComparison(ParseInt128(std::string(lhs)), ParseInt128(std::string(rhs)), kind);
        case ColumnType::String:
            return EvaluateComparison(std::string(lhs), std::string(rhs), kind);
        case ColumnType::Date:
            return EvaluateComparison(ParseDate(std::string(lhs)), ParseDate(std::string(rhs)), kind);
        case ColumnType::Timestamp:
            return EvaluateComparison(ParseTimestamp(std::string(lhs)), ParseTimestamp(std::string(rhs)), kind);
        case ColumnType::Character:
            return EvaluateComparison(ParseCharacter(std::string(lhs)), ParseCharacter(std::string(rhs)), kind);
    }
    throw Error::Unsupported("operators", "unsupported column type in comparison");
}

class ScanOperator final : public Operator {
   public:
    ScanOperator(std::filesystem::path path, std::vector<size_t> projection_indexes)
        : path_(std::move(path)), in_(OpenInputFile(path_)), metadata_(ColumnarBatchReader(path_).GetMetadata()) {
        if (metadata_.schema.columns.empty()) {
            throw Error::InvalidData("operators", "columnar schema is empty", path_.string());
        }

        projection_indexes_ = std::move(projection_indexes);
        projected_schema_.columns.reserve(projection_indexes_.size());

        for (const size_t source_index : projection_indexes_) {
            if (source_index >= metadata_.schema.columns.size()) {
                throw Error::OutOfRange("operators", "projection index out of range", path_.string());
            }
            projected_schema_.columns.push_back(metadata_.schema.columns[source_index]);
        }
    }

   public:
    std::optional<Batch> Next() override {
        if (next_group_ >= metadata_.row_groups.size()) {
            return std::nullopt;
        }

        const auto& row_group = metadata_.row_groups[next_group_++];
        Batch batch(projected_schema_, row_group.row_count);

        for (size_t projected_index = 0; projected_index < projection_indexes_.size(); ++projected_index) {
            const size_t source_index = projection_indexes_[projected_index];
            const auto& chunk = row_group.columns[source_index];
            SeekRead(path_, in_, chunk.offset);
            batch.ColumnAt(projected_index).ReadFrom(in_, row_group.row_count, chunk.size);
        }

        return batch;
    }

   private:
    std::filesystem::path path_;
    std::ifstream in_;

    ColumnarMetadata metadata_;
    std::vector<size_t> projection_indexes_;
    Schema projected_schema_;
    size_t next_group_ = 0;
};

class FilterOperator final : public Operator {
   public:
    FilterOperator(std::unique_ptr<Operator> child, PlannedFilter filter)
        : child_(std::move(child)), filter_(std::move(filter)) {}

   public:
    std::optional<Batch> Next() override {
        while (auto batch = child_->Next()) {
            Batch filtered(batch->GetSchema());

            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                const std::string value = batch->ColumnAt(filter_.column_index).ValueAsString(row);

                if (!MatchesComparison(filter_.column_type, value, filter_.literal_text, filter_.comparison)) {
                    continue;
                }

                for (size_t column_index = 0; column_index < batch->ColumnsCount(); ++column_index) {
                    filtered.ColumnAt(column_index).AppendFromString(batch->ColumnAt(column_index).ValueAsString(row));
                }
            }

            if (filtered.RowsCount() > 0) {
                return filtered;
            }
        }

        return std::nullopt;
    }

   private:
    std::unique_ptr<Operator> child_;
    PlannedFilter filter_;
};

struct AggBinding {
    PlannedAgg aggregate;
    std::unique_ptr<AggState> state;
};

class AggOperator final : public Operator {
   public:
    AggOperator(std::unique_ptr<Operator> child, const std::vector<PlannedAgg>& aggregates) : child_(std::move(child)) {
        bindings_.reserve(aggregates.size());
        for (const auto& aggregate : aggregates) {
            bindings_.push_back(AggBinding{
                .aggregate = aggregate,
                .state = CreateAggState(aggregate),
            });
        }
    }

   public:
    std::optional<Batch> Next() override {
        if (returned_) {
            return std::nullopt;
        }
        returned_ = true;

        while (auto batch = child_->Next()) {
            for (size_t row = 0; row < batch->RowsCount(); ++row) {
                for (auto& binding : bindings_) {
                    if (!binding.aggregate.star) {
                        std::string materialized = batch->ColumnAt(*binding.aggregate.column_index).ValueAsString(row);
                        binding.state->Consume(materialized);
                    } else {
                        binding.state->Consume(std::nullopt);
                    }
                }
            }
        }

        Schema schema;
        schema.columns.reserve(bindings_.size());

        for (const auto& binding : bindings_) {
            schema.columns.push_back(ColumnSchema{
                .name = binding.aggregate.output_name,
                .type = ColumnType::String,
            });
        }

        Batch result(schema);

        for (size_t i = 0; i < bindings_.size(); ++i) {
            result.ColumnAt(i).AppendFromString(bindings_[i].state->Finalize());
        }

        return result;
    }

   private:
    std::unique_ptr<Operator> child_;
    std::vector<AggBinding> bindings_;
    bool returned_ = false;
};

std::unique_ptr<Operator> BuildPlan(const PlannedQuery& planned) {
    std::unique_ptr<Operator> root = std::make_unique<ScanOperator>(planned.table_path, planned.projection_indexes);
    if (planned.filter.has_value()) {
        root = std::make_unique<FilterOperator>(std::move(root), *planned.filter);
    }
    root = std::make_unique<AggOperator>(std::move(root), planned.aggregates);
    return root;
}
