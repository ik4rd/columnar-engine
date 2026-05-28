#include "model/column.h"

#include <memory>

#include "model/column_boolean.h"
#include "model/column_character.h"
#include "model/column_date.h"
#include "model/column_int128.h"
#include "model/column_int16.h"
#include "model/column_int32.h"
#include "model/column_int64.h"
#include "model/column_string.h"
#include "model/column_timestamp.h"
#include "common/error.h"
#include "common/parsing.h"

std::unique_ptr<MutableColumn> CreateColumn(const ColumnType type) {
    switch (type) {
        case ColumnType::Int64:
            return std::make_unique<Int64Column>();
        case ColumnType::String:
            return std::make_unique<StringColumn>();
        case ColumnType::Boolean:
            return std::make_unique<BooleanColumn>();
        case ColumnType::Int16:
            return std::make_unique<Int16Column>();
        case ColumnType::Int32:
            return std::make_unique<Int32Column>();
        case ColumnType::Int128:
            return std::make_unique<Int128Column>();
        case ColumnType::Date:
            return std::make_unique<DateColumn>();
        case ColumnType::Timestamp:
            return std::make_unique<TimestampColumn>();
        case ColumnType::Character:
            return std::make_unique<CharacterColumn>();
    }

    throw Error::Unsupported("model", "unsupported column type");
}

Column::Column(const ColumnType type) : type_(type) {}

Int128 Column::ValueAsInt128(const size_t row) const { return ParseInt128(ValueAsString(row)); }

static bool MatchesValueComparison(const Int128 lhs, const Int128 rhs, const ValueComparison comparison) {
    switch (comparison) {
        case ValueComparison::Equal:
            return lhs == rhs;
        case ValueComparison::NotEqual:
            return lhs != rhs;
        case ValueComparison::Less:
            return lhs < rhs;
        case ValueComparison::LessOrEqual:
            return lhs <= rhs;
        case ValueComparison::Greater:
            return lhs > rhs;
        case ValueComparison::GreaterOrEqual:
            return lhs >= rhs;
    }

    return false;
}

void Column::SelectRowsByInt128Comparison(const Int128 rhs, const ValueComparison comparison,
                                          std::vector<size_t>& rows) const {
    for (size_t row = 0; row < Size(); ++row) {
        if (MatchesValueComparison(ValueAsInt128(row), rhs, comparison)) {
            rows.push_back(row);
        }
    }
}

void Column::SelectRowsByStringSet(const std::unordered_set<std::string>& values, std::vector<size_t>& rows) const {
    for (size_t row = 0; row < Size(); ++row) {
        if (values.contains(ValueAsString(row))) {
            rows.push_back(row);
        }
    }
}

void Column::SelectRowsByLikePattern(const std::string_view, const bool, std::vector<size_t>&) const {}

void Column::AppendEncodedValue(const size_t row, std::string& out) const {
    const std::string value = ValueAsString(row);
    out += std::to_string(value.size());
    out.push_back(':');
    out += value;
}

void Column::CheckRowIndex(const char* module, const size_t row, const size_t size) {
    if (row >= size) {
        throw Error::OutOfRange(module, "row index out of range");
    }
}
