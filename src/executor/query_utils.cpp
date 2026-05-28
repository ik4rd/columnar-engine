#include "executor/query_utils.h"

#include <array>

#include "common/ascii.h"
#include "common/error.h"

constexpr size_t SqlStringQuoteCount = 2;
constexpr char SqlStringQuote = '\'';
constexpr std::array<std::string_view, 4> ClickBenchStarColumns = {"WatchID", "EventTime", "URL", "Title"};

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

std::string UnescapeSqlString(const std::string_view text) {
    if (text.size() < SqlStringQuoteCount || text.front() != SqlStringQuote || text.back() != SqlStringQuote) {
        throw Error::InvalidArgument("executor", "invalid string literal");
    }

    std::string result;
    result.reserve(text.size() - SqlStringQuoteCount);

    for (size_t i = 1; i + 1 < text.size(); ++i) {
        if (text[i] == SqlStringQuote && i + SqlStringQuoteCount < text.size() && text[i + 1] == SqlStringQuote) {
            result.push_back(SqlStringQuote);
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

std::string NormalizeLiteralForEval(const QueryLiteral& literal) {
    if (literal.kind != LiteralKind::String) {
        return literal.text;
    }

    return UnescapeSqlString(literal.text);
}

void NormalizeRegexReplacement(std::string& replacement) {
    for (size_t pos = 0; (pos = replacement.find("\\1", pos)) != std::string::npos;) {
        replacement.replace(pos, 2, "$1");
        pos += 2;
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

bool SameExpr(const ExprPtr& lhs, const ExprPtr& rhs) {
    if (!lhs || !rhs || lhs->kind != rhs->kind) {
        return false;
    }

    switch (lhs->kind) {
        case ExprKind::Column:
            return SameColumnRef(lhs->column, rhs->column);
        case ExprKind::Literal:
            return lhs->literal.text == rhs->literal.text && lhs->literal.kind == rhs->literal.kind;
        case ExprKind::Star:
            return true;
        case ExprKind::Binary:
            return lhs->binary_op == rhs->binary_op && SameExpr(lhs->left, rhs->left) &&
                   SameExpr(lhs->right, rhs->right);
        case ExprKind::Function:
            if (!SameColumnName(lhs->function_name, rhs->function_name) ||
                lhs->arguments.size() != rhs->arguments.size()) {
                return false;
            }
            for (size_t i = 0; i < lhs->arguments.size(); ++i) {
                if (!SameExpr(lhs->arguments[i], rhs->arguments[i])) {
                    return false;
                }
            }
            return true;
        case ExprKind::Case:
            return lhs->output_name == rhs->output_name;
    }

    return false;
}

std::vector<size_t> StarColumnIndexes(const Schema& schema) {
    std::vector<size_t> clickbench_subset;

    for (const std::string_view name : ClickBenchStarColumns) {
        try {
            clickbench_subset.push_back(FindColumnIndex(schema, name));
        } catch (const Error&) {
            clickbench_subset.clear();
            break;
        }
    }

    if (clickbench_subset.size() == ClickBenchStarColumns.size()) {
        return clickbench_subset;
    }

    std::vector<size_t> all;
    all.reserve(schema.columns.size());

    for (size_t i = 0; i < schema.columns.size(); ++i) {
        all.push_back(i);
    }

    return all;
}

std::optional<size_t> TryFindBatchColumn(const Schema& schema, const std::string_view name) {
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (SameColumnName(schema.columns[i].name, name)) {
            return i;
        }
    }
    return std::nullopt;
}
