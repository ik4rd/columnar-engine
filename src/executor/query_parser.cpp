#include <array>
#include <utility>

#include "executor.h"

class TokenCursor {
   public:
    explicit TokenCursor(std::vector<TokenPtr> tokens) : tokens_(std::move(tokens)) {}

   public:
    const Token& Peek() const {
        if (pos_ >= tokens_.size()) {
            throw Error::InvalidArgument("query_parser", "unexpected end of query");
        }
        return *tokens_[pos_];
    }

    bool Match(const Tokens type) const { return pos_ < tokens_.size() && tokens_[pos_]->GetType() == type; }

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
            case Tokens::EOI:
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

ParsedQuery ParseQuery(const std::string_view query) {
    auto tokenized = TokenizeSql(query);
    if (!tokenized.has_value()) {
        throw tokenized.error();
    }

    TokenCursor cursor(std::move(tokenized.value()));

    ParsedQuery parsed;
    cursor.Consume(Tokens::Select, "expected SELECT");

    parsed.aggregates.push_back(ParseAggregate(cursor));
    while (cursor.TryConsume(Tokens::Comma)) {
        parsed.aggregates.push_back(ParseAggregate(cursor));
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

        parsed.filter = std::move(filter);
    }

    cursor.TryConsume(Tokens::Semicolon);
    cursor.Consume(Tokens::EOI, "unexpected trailing tokens");

    return parsed;
}
