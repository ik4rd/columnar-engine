#include "executor/operator_utils.h"

#include <compare>
#include <string>

#include "common/ascii.h"
#include "common/error.h"
#include "model/column_traits.h"

static bool EvaluateOrdering(const std::strong_ordering ordering, const ComparisonKind kind) {
    if (kind == ComparisonKind::Equal) {
        return ordering == 0;
    }

    if (kind == ComparisonKind::NotEqual) {
        return ordering != 0;
    }

    if (kind == ComparisonKind::Less) {
        return ordering < 0;
    }

    if (kind == ComparisonKind::LessOrEqual) {
        return ordering <= 0;
    }

    if (kind == ComparisonKind::Greater) {
        return ordering > 0;
    }

    if (kind == ComparisonKind::GreaterOrEqual) {
        return ordering >= 0;
    }

    throw Error::Unsupported("executor", "unsupported comparison kind");
}

template <typename T>
static std::strong_ordering CompareValuesImpl(const T left, const T right) {
    return left <=> right;
}

std::strong_ordering CompareValues(const ColumnType type, const std::string_view lhs, const std::string_view rhs) {
    return VisitColumnType(type, [&]<ColumnType TypeValue>() {
        return CompareValuesImpl(ColumnValueTraits<TypeValue>::Parse(lhs), ColumnValueTraits<TypeValue>::Parse(rhs));
    });
}

bool MatchesComparison(const ColumnType type, const std::string_view lhs, const std::string_view rhs,
                       const ComparisonKind kind) {
    return EvaluateOrdering(CompareValues(type, lhs, rhs), kind);
}

ColumnType AggregateOutputType(const PlannedAgg& aggregate) {
    const std::string name = ToUpperAscii(aggregate.function->canonical_name);

    if (name == "COUNT") {
        return ColumnType::Int64;
    }

    if (name == "SUM" || name == "AVG") {
        return ColumnType::Int128;
    }

    if (name == "MIN" || name == "MAX") {
        return aggregate.input_type;
    }

    throw Error::Unsupported("executor", "unsupported aggregate result type");
}
