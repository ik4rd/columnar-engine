#include "executor/operator_utils.h"

#include <string>

#include "common/ascii.h"
#include "common/error.h"
#include "common/parsing.h"

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
    throw Error::Unsupported("executor", "unsupported comparison kind");
}

template <typename T>
static int CompareValuesImpl(const T left, const T right) {
    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

int CompareValues(const ColumnType type, const std::string_view lhs, const std::string_view rhs) {
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
    return EvaluateComparison(CompareValues(type, lhs, rhs), 0, kind);
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
