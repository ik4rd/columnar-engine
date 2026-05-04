#include <array>
#include <utility>

#include "executor/executor.h"
#include "support/ascii.h"

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

static AggSpec ParseAggregate(TokenCursor& cursor) {
    const Token& function_token = cursor.ConsumeFunctionToken("expected aggregate function");

    AggSpec spec;
    spec.function = &ResolveAggFunc(function_token.GetText());

    cursor.Consume(Tokens::OpenBracket, "expected '(' after aggregate function");

    if (cursor.TryConsume(Tokens::Asterisk)) {
        if (!spec.function->star) {
            throw Error::InvalidArgument("query_parser",
                                         std::string(spec.function->canonical_name) + " does not support '*'");
        }
        spec.star = true;
    } else {
        spec.distinct = cursor.TryConsume(Tokens::Distinct);
        if (spec.distinct && !spec.function->distinct) {
            throw Error::InvalidArgument("query_parser",
                                         std::string(spec.function->canonical_name) + " does not support DISTINCT");
        }
        const auto& token = cursor.Consume(Tokens::NameToken, "expected column name in aggregate");
        spec.column_name = std::string(token.GetText());
    }

    cursor.Consume(Tokens::CloseBracket, "expected ')' after aggregate arguments");
    spec.output_name = FormatAgg(*spec.function, spec.column_name, spec.distinct, spec.star);

    return spec;
}

static SelectItemSpec ParseSelectItem(TokenCursor& cursor) {
    if (cursor.Match(Tokens::NameToken) && !cursor.Match(Tokens::OpenBracket, 1)) {
        const Token& token = cursor.Consume(Tokens::NameToken, "expected column name in SELECT");
        return SelectItemSpec{
            .kind = SelectItemKind::GroupKey,
            .column_name = std::string(token.GetText()),
            .aggregate = {},
            .output_name = std::string(token.GetText()),
        };
    }

    return SelectItemSpec{
        .kind = SelectItemKind::Aggregate,
        .column_name = {},
        .aggregate = ParseAggregate(cursor),
        .output_name = {},
    };
}

static OrderBySpec ParseOrderByItem(TokenCursor& cursor) {
    SelectItemSpec item = ParseSelectItem(cursor);
    if (item.kind == SelectItemKind::Aggregate) {
        item.output_name = item.aggregate.output_name;
    }

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

ParsedQuery ParseQuery(const std::string_view query) {
    auto tokenized = TokenizeSql(query);
    if (!tokenized.has_value()) {
        throw tokenized.error();
    }

    TokenCursor cursor(std::move(tokenized.value()));

    ParsedQuery parsed;
    cursor.Consume(Tokens::Select, "expected SELECT");

    parsed.select_items.push_back(ParseSelectItem(cursor));
    while (cursor.TryConsume(Tokens::Comma)) {
        parsed.select_items.push_back(ParseSelectItem(cursor));
    }

    for (auto& item : parsed.select_items) {
        if (item.kind == SelectItemKind::Aggregate) {
            item.output_name = item.aggregate.output_name;
        }
    }

    cursor.Consume(Tokens::From, "expected FROM");
    parsed.table_name = std::string(cursor.Consume(Tokens::NameToken, "expected table name").GetText());

    if (cursor.TryConsume(Tokens::Where)) {
        FilterSpec filter;
        filter.column_name =
            std::string(cursor.Consume(Tokens::NameToken, "expected column name after WHERE").GetText());

        const Tokens comparison_token = cursor.Peek().GetType();
        filter.comparison = ComparisonKindFromToken(comparison_token);

        cursor.Consume(comparison_token, "expected comparison operator in WHERE");

        if (cursor.TryConsume(Tokens::Minus)) {
            const Token& literal = cursor.Consume(Tokens::NumericLiteral, "expected numeric literal after '-'");
            filter.literal =
                ParsedLiteral{.text = "-" + std::string(literal.GetText()), .token_type = Tokens::NumericLiteral};
        } else {
            const Token& literal = cursor.Peek();
            switch (literal.GetType()) {
                case Tokens::NumericLiteral:
                case Tokens::StringLiteral:
                case Tokens::NameToken:
                    filter.literal = ParsedLiteral{
                        .text = std::string(literal.GetText()),
                        .token_type = literal.GetType(),
                    };
                    cursor.Consume(literal.GetType(), "expected literal after comparison operator");
                    break;
                default:
                    throw Error::InvalidArgument("query_parser", "unsupported literal in WHERE");
            }
        }

        parsed.filter = std::move(filter);
    }

    if (cursor.TryConsume(Tokens::Group)) {
        cursor.Consume(Tokens::By, "expected BY after GROUP");
        parsed.group_by.push_back(std::string(cursor.Consume(Tokens::NameToken, "expected column name in GROUP BY").GetText()));
        while (cursor.TryConsume(Tokens::Comma)) {
            parsed.group_by.push_back(
                std::string(cursor.Consume(Tokens::NameToken, "expected column name in GROUP BY").GetText()));
        }
    }

    if (cursor.TryConsume(Tokens::Order)) {
        cursor.Consume(Tokens::By, "expected BY after ORDER");
        parsed.order_by.push_back(ParseOrderByItem(cursor));
        while (cursor.TryConsume(Tokens::Comma)) {
            parsed.order_by.push_back(ParseOrderByItem(cursor));
        }
    }

    cursor.TryConsume(Tokens::Semicolon);
    cursor.Consume(Tokens::EndOfInput, "unexpected trailing tokens");

    return parsed;
}
