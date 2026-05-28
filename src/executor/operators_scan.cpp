#include <algorithm>
#include <utility>

#include "common/ascii.h"
#include "common/error.h"
#include "common/operator_utils.h"
#include "executor/aggregate_state.h"
#include "executor/operators_internal.h"
#include "io/columnar_batch.h"
#include "io/file.h"

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

bool MatchesRowGroupComparison(const ColumnChunkMetadata& chunk, const Int128 value, const ComparisonKind comparison) {
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

bool MayMatchRowGroup(const PredicatePtr& predicate, const RowGroupMetadata& row_group) {
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

            return std::any_of(predicate->metadata_typed_in_values.begin(), predicate->metadata_typed_in_values.end(),
                               [&](const Int128 value) {
                                   return MatchesRowGroupComparison(
                                       row_group.columns[predicate->metadata_typed_in_column_index], value,
                                       ComparisonKind::Equal);
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

            ReadBatchColumnChunk(path_, input_, chunk, row_group->row_count, batch, projected_index);
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

std::unique_ptr<Operator> CreateMetadataCountOperator(std::filesystem::path path, std::string output_name) {
    return std::make_unique<MetadataCountOperator>(std::move(path), std::move(output_name));
}

std::unique_ptr<Operator> CreateMetadataExtremaOperator(std::filesystem::path path,
                                                        std::vector<PlannedAgg> aggregates) {
    return std::make_unique<MetadataExtremaOperator>(std::move(path), std::move(aggregates));
}

std::unique_ptr<Operator> CreateScanOperator(std::filesystem::path path, std::vector<size_t> projection_indexes,
                                             PredicatePtr filter) {
    return std::make_unique<ScanOperator>(std::move(path), std::move(projection_indexes), std::move(filter));
}
