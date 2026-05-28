#include "executor/typed_value_utils.h"

#include <string>

#include "common/error.h"
#include "common/parsing.h"
#include "executor/query_utils.h"

bool IsNumericColumnType(const ColumnType type) {
    switch (type) {
        case ColumnType::Boolean:
        case ColumnType::Int16:
        case ColumnType::Int32:
        case ColumnType::Int64:
        case ColumnType::Int128:
        case ColumnType::Character:
            return true;
        case ColumnType::String:
        case ColumnType::Date:
        case ColumnType::Timestamp:
            return false;
    }

    return false;
}

Int128 ParseColumnValueAsInt128(const ColumnType type, const std::string_view value) {
    switch (type) {
        case ColumnType::Boolean:
            return ParseBoolean(value) ? 1 : 0;
        case ColumnType::Int16:
            return ParseInt16(value);
        case ColumnType::Int32:
            return ParseInt32(value);
        case ColumnType::Int64:
            return ParseInt64(value);
        case ColumnType::Int128:
            return ParseInt128(value);
        case ColumnType::Date:
            return ParseDate(value);
        case ColumnType::Timestamp:
            return ParseTimestamp(value);
        case ColumnType::Character:
            return ParseCharacter(value);
        case ColumnType::String:
            break;
    }

    throw Error::Unsupported("executor", "unsupported typed value conversion");
}

std::optional<Int128> TryParseLiteralValueAsInt128(const QueryLiteral& literal, const ColumnType type) {
    try {
        if (literal.kind == LiteralKind::Numeric) {
            return ParseInt128(literal.text);
        }

        if (literal.kind != LiteralKind::String) {
            return std::nullopt;
        }

        return ParseColumnValueAsInt128(type, NormalizeLiteralForEval(literal));
    } catch (const Error&) {
        return std::nullopt;
    }
}

std::string FormatInt128Value(const ColumnType type, const Int128 value) {
    switch (type) {
        case ColumnType::Boolean:
            return BooleanToString(value != 0);
        case ColumnType::Int16:
        case ColumnType::Int32:
        case ColumnType::Int64:
        case ColumnType::Int128:
            return Int128ToString(value);
        case ColumnType::Date:
            return DateToString(static_cast<int32_t>(value));
        case ColumnType::Timestamp:
            return TimestampToString(static_cast<int64_t>(value));
        case ColumnType::Character:
            return std::string(1, static_cast<char>(value));
        case ColumnType::String:
            break;
    }

    throw Error::Unsupported("executor", "unsupported typed value formatting");
}

ComparisonKind FlipComparison(const ComparisonKind comparison) {
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
