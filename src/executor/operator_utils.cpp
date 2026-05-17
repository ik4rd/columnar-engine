#include "executor/operator_utils.h"

#include <compare>
#include <string>

#include "common/ascii.h"
#include "common/error.h"
#include "common/parsing.h"

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
    switch (type) {
        case ColumnType::Boolean:
            return CompareValuesImpl(ParseBoolean(std::string(lhs)), ParseBoolean(std::string(rhs)));
        case ColumnType::Int16:
            return CompareValuesImpl(ParseInt16(std::string(lhs)), ParseInt16(std::string(rhs)));
        case ColumnType::Int32:
            return CompareValuesImpl(ParseInt32(std::string(lhs)), ParseInt32(std::string(rhs)));
        case ColumnType::Int64:
            return CompareValuesImpl(ParseInt64(std::string(lhs)), ParseInt64(std::string(rhs)));
        case ColumnType::Int128:
            return CompareValuesImpl(ParseInt128(std::string(lhs)), ParseInt128(std::string(rhs)));
        case ColumnType::String:
            return CompareValuesImpl(std::string(lhs), std::string(rhs));
        case ColumnType::Date:
            return CompareValuesImpl(ParseDate(std::string(lhs)), ParseDate(std::string(rhs)));
        case ColumnType::Timestamp:
            return CompareValuesImpl(ParseTimestamp(std::string(lhs)), ParseTimestamp(std::string(rhs)));
        case ColumnType::Character:
            return CompareValuesImpl(ParseCharacter(std::string(lhs)), ParseCharacter(std::string(rhs)));
    }

    throw Error::Unsupported("executor", "unsupported column type in comparison");
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
