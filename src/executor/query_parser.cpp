#include <array>
#include <charconv>
#include <utility>
#include <vector>

#include "executor/query_parser.h"
#include "sql_parser/tokenizer.h"
#include "support/ascii.h"
#include "support/error.h"

class TokenCursor {
   public:
    explicit TokenCursor(std::vector<TokenPtr> tokens) : tokens_(std::move(tokens)) {}

   public:
    const Token& Peek(const size_t lookahead = 0) const {
        if (pos_ + lookahead >= tokens_.size()) {
            throw Error::InvalidArgument("query_parser", "unexpected end of query");
        }
        return *tokens_[pos_ + lookahead];
    }

    bool Match(const Tokens type, const size_t lookahead = 0) const {
        return pos_ + lookahead < tokens_.size() && tokens_[pos_ + lookahead]->GetType() == type;
    }

    const Token& Consume(const Tokens type, const std::string_view message) {
        if (!Match(type)) {
            throw Error::InvalidArgument("query_parser", std::string(message));
        }
        return *tokens_[pos_++];
    }

    bool TryConsume(const Tokens type) {
        if (!Match(type)) {
            return false;
        }
        ++pos_;
        return true;
    }

    const Token& ConsumeFunctionToken(const std::string_view message) {
        if (pos_ >= tokens_.size()) {
            throw Error::InvalidArgument("query_parser", std::string(message));
        }

        const Tokens type = tokens_[pos_]->GetType();
        switch (type) {
            case Tokens::NumericLiteral:
            case Tokens::StringLiteral:
            case Tokens::OpenBracket:
            case Tokens::CloseBracket:
            case Tokens::Comma:
            case Tokens::Dot:
            case Tokens::Semicolon:
            case Tokens::Equal:
            case Tokens::NotEqual:
            case Tokens::Less:
            case Tokens::LessOrEqual:
            case Tokens::Greater:
            case Tokens::GreaterOrEqual:
            case Tokens::EndOfInput:
                throw Error::InvalidArgument("query_parser", std::string(message));
            default:
                break;
        }

        return *tokens_[pos_++];
    }

   private:
    std::vector<TokenPtr> tokens_;
    size_t pos_ = 0;
};

static ComparisonKind ComparisonKindFromToken(const Tokens type) {
    switch (type) {
        case Tokens::Equal:
            return ComparisonKind::Equal;
        case Tokens::NotEqual:
            return ComparisonKind::NotEqual;
        case Tokens::Less:
            return ComparisonKind::Less;
        case Tokens::LessOrEqual:
            return ComparisonKind::LessOrEqual;
        case Tokens::Greater:
            return ComparisonKind::Greater;
        case Tokens::GreaterOrEqual:
            return ComparisonKind::GreaterOrEqual;
        default:
            throw Error::InvalidArgument("query_parser", "expected comparison operator in WHERE");
    }
}

static std::string FormatColumnRef(const ColumnRef& column) {
    if (column.qualifier.empty()) {
        return column.name;
    }
    return column.qualifier + "." + column.name;
}

static ColumnRef ParseColumnRef(TokenCursor& cursor, const std::string_view message) {
    ColumnRef column{
        .qualifier = {},
        .name = std::string(cursor.Consume(Tokens::NameToken, message).GetText()),
    };

    if (cursor.TryConsume(Tokens::Dot)) {
        column.qualifier = std::move(column.name);
        column.name = std::string(cursor.Consume(Tokens::NameToken, "expected column name after '.'").GetText());
    }

    return column;
}

static QueryValue QueryColumnValue(ColumnRef column) {
    return QueryValue{
        .kind = QueryValueKind::Column,
        .column = std::move(column),
        .literal = {},
    };
}

static QueryValue QueryLiteralValue(QueryLiteral literal) {
    return QueryValue{
        .kind = QueryValueKind::Literal,
        .column = {},
        .literal = std::move(literal),
    };
}

static QueryValue ParseFilterValue(TokenCursor& cursor, const std::string_view message,
                                   const bool bare_identifier_is_column) {
    if (cursor.TryConsume(Tokens::Minus)) {
        const Token& literal = cursor.Consume(Tokens::NumericLiteral, "expected numeric literal after '-'");
        return QueryLiteralValue(QueryLiteral{
            .text = "-" + std::string(literal.GetText()),
            .kind = LiteralKind::Numeric,
        });
    }

    const Token& token = cursor.Peek();
    switch (token.GetType()) {
        case Tokens::NumericLiteral:
            cursor.Consume(Tokens::NumericLiteral, message);
            return QueryLiteralValue(QueryLiteral{
                .text = std::string(token.GetText()),
                .kind = LiteralKind::Numeric,
            });
        case Tokens::StringLiteral:
            cursor.Consume(Tokens::StringLiteral, message);
            return QueryLiteralValue(QueryLiteral{
                .text = std::string(token.GetText()),
                .kind = LiteralKind::String,
            });
        case Tokens::NameToken:
            if (bare_identifier_is_column || cursor.Match(Tokens::Dot, 1)) {
                return QueryColumnValue(ParseColumnRef(cursor, message));
            }
            cursor.Consume(Tokens::NameToken, message);
            return QueryLiteralValue(QueryLiteral{
                .text = std::string(token.GetText()),
                .kind = LiteralKind::Identifier,
            });
        default:
            throw Error::InvalidArgument("query_parser", std::string(message));
    }
}

static std::string FormatAggregateCall(const std::string_view function_name, const std::string_view column_name,
                                       const bool distinct, const AggArgumentKind argument_kind) {
    std::string output(function_name);
    output += '(';
    if (argument_kind == AggArgumentKind::Star) {
        output += '*';
    } else {
        if (distinct) {
            output += "DISTINCT ";
        }
        output += column_name;
    }
    output += ')';
    return output;
}

static std::optional<size_t> ParseOptionalLimit(TokenCursor& cursor) {
    if (!cursor.TryConsume(Tokens::Limit)) {
        return std::nullopt;
    }

    const Token& limit_token = cursor.Consume(Tokens::NumericLiteral, "expected numeric literal after LIMIT");
    size_t limit = 0;
    const std::string_view text = limit_token.GetText();
    const char* begin = text.data();
    const char* end = begin + text.size();
    if (const auto [ptr, ec] = std::from_chars(begin, end, limit); ec != std::errc() || ptr != end) {
        throw Error::InvalidArgument("query_parser", "invalid LIMIT value");
    }

    return limit;
}

static AggSpec ParseAggregate(TokenCursor& cursor) {
    const Token& function_token = cursor.ConsumeFunctionToken("expected aggregate function");

    AggSpec spec;
    spec.function_name = ToUpperAscii(function_token.GetText());

    cursor.Consume(Tokens::OpenBracket, "expected '(' after aggregate function");

    if (cursor.TryConsume(Tokens::Asterisk)) {
        spec.argument_kind = AggArgumentKind::Star;
    } else {
        spec.distinct = cursor.TryConsume(Tokens::Distinct);
        spec.column = ParseColumnRef(cursor, "expected column name in aggregate");
    }

    cursor.Consume(Tokens::CloseBracket, "expected ')' after aggregate arguments");
    spec.output_name = FormatAggregateCall(spec.function_name, FormatColumnRef(spec.column), spec.distinct,
                                           spec.argument_kind);

    return spec;
}

static SelectItemSpec ParseSelectExpression(TokenCursor& cursor) {
    if (cursor.Match(Tokens::NameToken) && !cursor.Match(Tokens::OpenBracket, 1)) {
        const ColumnRef column = ParseColumnRef(cursor, "expected column name in SELECT");
        return SelectItemSpec{
            .kind = SelectItemKind::GroupKey,
            .column = column,
            .aggregate = {},
            .output_name = column.name,
        };
    }

    return SelectItemSpec{
        .kind = SelectItemKind::Aggregate,
        .column = {},
        .aggregate = ParseAggregate(cursor),
        .output_name = {},
    };
}

static std::string ParseSelectItemAlias(TokenCursor& cursor) {
    if (cursor.TryConsume(Tokens::As)) {
        return std::string(cursor.Consume(Tokens::NameToken, "expected alias after AS").GetText());
    }
    if (cursor.Match(Tokens::NameToken)) {
        return std::string(cursor.Consume(Tokens::NameToken, "expected alias").GetText());
    }
    return {};
}

static SelectItemSpec ParseSelectItem(TokenCursor& cursor) {
    SelectItemSpec item = ParseSelectExpression(cursor);
    if (const std::string alias = ParseSelectItemAlias(cursor); !alias.empty()) {
        item.output_name = alias;
    } else if (item.kind == SelectItemKind::Aggregate) {
        item.output_name = item.aggregate.output_name;
    }
    return item;
}

static OrderBySpec ParseOrderByItem(TokenCursor& cursor) {
    SelectItemSpec item = ParseSelectExpression(cursor);
    item.output_name = item.kind == SelectItemKind::Aggregate ? item.aggregate.output_name : item.column.name;

    bool descending = false;
    if (cursor.Match(Tokens::NameToken)) {
        const std::string direction = ToUpperAscii(cursor.Peek().GetText());
        if (direction == "DESC") {
            cursor.Consume(Tokens::NameToken, "expected DESC");
            descending = true;
        } else if (direction == "ASC") {
            cursor.Consume(Tokens::NameToken, "expected ASC");
        }
    }

    return OrderBySpec{
        .item = std::move(item),
        .descending = descending,
    };
}

Query ParseQuery(const std::string_view query) {
    auto tokenized = TokenizeSql(query);
    if (!tokenized.has_value()) {
        throw tokenized.error();
    }

    TokenCursor cursor(std::move(tokenized.value()));

    Query query_ast;
    cursor.Consume(Tokens::Select, "expected SELECT");

    query_ast.select_items.push_back(ParseSelectItem(cursor));
    while (cursor.TryConsume(Tokens::Comma)) {
        query_ast.select_items.push_back(ParseSelectItem(cursor));
    }

    cursor.Consume(Tokens::From, "expected FROM");
    query_ast.table_name = std::string(cursor.Consume(Tokens::NameToken, "expected table name").GetText());
    if (cursor.TryConsume(Tokens::As)) {
        query_ast.table_alias = std::string(cursor.Consume(Tokens::NameToken, "expected alias after AS").GetText());
    } else if (cursor.Match(Tokens::NameToken)) {
        query_ast.table_alias = std::string(cursor.Consume(Tokens::NameToken, "expected table alias").GetText());
    }

    if (cursor.TryConsume(Tokens::Where)) {
        FilterSpec filter;
        filter.left = ParseFilterValue(cursor, "expected left operand after WHERE", true);

        const Tokens comparison_token = cursor.Peek().GetType();
        filter.comparison = ComparisonKindFromToken(comparison_token);

        cursor.Consume(comparison_token, "expected comparison operator in WHERE");
        filter.right = ParseFilterValue(cursor, "expected right operand after comparison operator", false);

        query_ast.filter = std::move(filter);
    }

    if (cursor.TryConsume(Tokens::Group)) {
        cursor.Consume(Tokens::By, "expected BY after GROUP");
        query_ast.group_by.push_back(ParseColumnRef(cursor, "expected column name in GROUP BY"));
        while (cursor.TryConsume(Tokens::Comma)) {
            query_ast.group_by.push_back(ParseColumnRef(cursor, "expected column name in GROUP BY"));
        }
    }

    if (cursor.TryConsume(Tokens::Order)) {
        cursor.Consume(Tokens::By, "expected BY after ORDER");
        query_ast.order_by.push_back(ParseOrderByItem(cursor));
        while (cursor.TryConsume(Tokens::Comma)) {
            query_ast.order_by.push_back(ParseOrderByItem(cursor));
        }
    }

    query_ast.limit = ParseOptionalLimit(cursor);
    cursor.TryConsume(Tokens::Semicolon);
    cursor.Consume(Tokens::EndOfInput, "unexpected trailing tokens");

    return query_ast;
}
