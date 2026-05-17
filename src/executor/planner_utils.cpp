#include "executor/planner_utils.h"

#include "common/ascii.h"
#include "common/error.h"

size_t FindColumnIndex(const Schema& schema, const std::string_view column_name) {
    const std::string needle = ToLowerAscii(column_name);

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (ToLowerAscii(schema.columns[i].name) == needle) {
            return i;
        }
    }

    throw Error::InvalidArgument("executor", "unknown column '" + std::string(column_name) + "'");
}

static bool SameQualifier(const std::string_view lhs, const std::string_view rhs) {
    return ToLowerAscii(lhs) == ToLowerAscii(rhs);
}

void ValidateColumnQualifier(const Query& query, const ColumnRef& column) {
    if (column.qualifier.empty()) {
        return;
    }

    if (SameQualifier(column.qualifier, query.table_name)) {
        return;
    }

    if (!query.table_alias.empty() && SameQualifier(column.qualifier, query.table_alias)) {
        return;
    }

    throw Error::InvalidArgument("executor", "unknown table qualifier '" + column.qualifier + "'");
}

static std::string UnescapeSqlString(const std::string_view text) {
    if (text.size() < 2 || text.front() != '\'' || text.back() != '\'') {
        throw Error::InvalidArgument("executor", "invalid string literal");
    }

    std::string result;
    result.reserve(text.size() - 2);

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

std::string NormalizeLiteral(const QueryLiteral& literal, const ColumnType type) {
    switch (literal.kind) {
        case LiteralKind::String:
            return UnescapeSqlString(literal.text);
        case LiteralKind::Numeric:
        case LiteralKind::Identifier:
            if (type == ColumnType::String && literal.kind == LiteralKind::Identifier) {
                return literal.text;
            }
            return literal.text;
        default:
            throw Error::InvalidArgument("executor", "unsupported literal type");
    }
}

bool SameColumnName(const std::string_view lhs, const std::string_view rhs) {
    return ToLowerAscii(lhs) == ToLowerAscii(rhs);
}

bool SameColumnRef(const ColumnRef& lhs, const ColumnRef& rhs) {
    if (!SameColumnName(lhs.name, rhs.name)) {
        return false;
    }

    if (lhs.qualifier.empty() || rhs.qualifier.empty()) {
        return true;
    }

    return SameQualifier(lhs.qualifier, rhs.qualifier);
}

bool SameSelectItem(const SelectItemSpec& lhs, const SelectItemSpec& rhs) {
    if (lhs.kind != rhs.kind) {
        return false;
    }

    if (lhs.kind == SelectItemKind::GroupKey) {
        return SameColumnRef(lhs.column, rhs.column);
    }

    return ToLowerAscii(lhs.aggregate.output_name) == ToLowerAscii(rhs.aggregate.output_name);
}

bool SameOutputName(const SelectItemSpec& lhs, const SelectItemSpec& rhs) {
    return SameColumnName(lhs.output_name, rhs.output_name);
}
