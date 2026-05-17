#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/ascii.h"
#include "sql_parser/tokenizer.h"

static const std::unordered_map<std::string, Tokens> Keywords = {
    {"AS", Tokens::As},       {"AND", Tokens::And},       {"AVG", Tokens::Avg},           {"BY", Tokens::By},
    {"COUNT", Tokens::Count}, {"CREATE", Tokens::Create}, {"DISTINCT", Tokens::Distinct}, {"FROM", Tokens::From},
    {"GROUP", Tokens::Group}, {"LENGTH", Tokens::Length}, {"LIMIT", Tokens::Limit},       {"MAX", Tokens::Max},
    {"MIN", Tokens::Min},     {"ORDER", Tokens::Order},   {"SELECT", Tokens::Select},     {"SUM", Tokens::Sum},
    {"WHERE", Tokens::Where},
};

static std::string DescribeCharacter(const char ch) {
    switch (ch) {
        case '\n':
            return "\\n";
        case '\r':
            return "\\r";
        case '\t':
            return "\\t";
        default:
            break;
    }

    if (std::isprint(static_cast<unsigned char>(ch)) != 0) {
        return std::string(1, ch);
    }

    return "code " + std::to_string(static_cast<unsigned>(static_cast<unsigned char>(ch)));
}

Tokens ResolveIdentifierType(const std::string_view text) {
    const std::string upper = ToUpperAscii(text);
    const auto it = Keywords.find(upper);

    if (it != Keywords.end()) {
        return it->second;
    }

    return Tokens::NameToken;
}

TokenPtr MakeToken(const Tokens type, std::string text, const size_t offset) {
    switch (type) {
        case Tokens::NameToken:
            return std::make_shared<TNameToken>(std::move(text), offset);
        case Tokens::NumericLiteral:
            return std::make_shared<TNumericLiteralToken>(std::move(text), offset);
        case Tokens::StringLiteral:
            return std::make_shared<TStringLiteralToken>(std::move(text), offset);
        case Tokens::Select:
            return std::make_shared<TSelectToken>(std::move(text), offset);
        case Tokens::From:
            return std::make_shared<TFromToken>(std::move(text), offset);
        case Tokens::Create:
            return std::make_shared<TCreateToken>(std::move(text), offset);
        case Tokens::As:
            return std::make_shared<TAsToken>(std::move(text), offset);
        case Tokens::Sum:
            return std::make_shared<TSumToken>(std::move(text), offset);
        case Tokens::Count:
            return std::make_shared<TCountToken>(std::move(text), offset);
        case Tokens::Avg:
            return std::make_shared<TAvgToken>(std::move(text), offset);
        case Tokens::Max:
            return std::make_shared<TMaxToken>(std::move(text), offset);
        case Tokens::Min:
            return std::make_shared<TMinToken>(std::move(text), offset);
        case Tokens::And:
            return std::make_shared<TAndToken>(std::move(text), offset);
        case Tokens::Distinct:
            return std::make_shared<TDistinctToken>(std::move(text), offset);
        case Tokens::Length:
            return std::make_shared<TLengthToken>(std::move(text), offset);
        case Tokens::Where:
            return std::make_shared<TWhereToken>(std::move(text), offset);
        case Tokens::By:
            return std::make_shared<TByToken>(std::move(text), offset);
        case Tokens::Group:
            return std::make_shared<TGroupToken>(std::move(text), offset);
        case Tokens::Order:
            return std::make_shared<TOrderToken>(std::move(text), offset);
        case Tokens::Limit:
            return std::make_shared<TLimitToken>(std::move(text), offset);
        case Tokens::Plus:
            return std::make_shared<TPlusToken>(std::move(text), offset);
        case Tokens::Minus:
            return std::make_shared<TMinusToken>(std::move(text), offset);
        case Tokens::Asterisk:
            return std::make_shared<TAsteriskToken>(std::move(text), offset);
        case Tokens::OpenBracket:
            return std::make_shared<TOpenBracketToken>(std::move(text), offset);
        case Tokens::CloseBracket:
            return std::make_shared<TCloseBracketToken>(std::move(text), offset);
        case Tokens::Comma:
            return std::make_shared<TCommaToken>(std::move(text), offset);
        case Tokens::Dot:
            return std::make_shared<TDotToken>(std::move(text), offset);
        case Tokens::Semicolon:
            return std::make_shared<TSemicolonToken>(std::move(text), offset);
        case Tokens::Equal:
            return std::make_shared<TEqualToken>(std::move(text), offset);
        case Tokens::NotEqual:
            return std::make_shared<TNotEqualToken>(std::move(text), offset);
        case Tokens::Less:
            return std::make_shared<TLessToken>(std::move(text), offset);
        case Tokens::LessOrEqual:
            return std::make_shared<TLessOrEqualToken>(std::move(text), offset);
        case Tokens::Greater:
            return std::make_shared<TGreaterToken>(std::move(text), offset);
        case Tokens::GreaterOrEqual:
            return std::make_shared<TGreaterOrEqualToken>(std::move(text), offset);
        case Tokens::EndOfInput:
            return std::make_shared<TEndOfInputToken>(std::move(text), offset);
    }

    return nullptr;
}

Expected<TokenPtr> MakeUnexpectedCharacter(const char ch, const size_t offset) {
    return tl::unexpected(Error::MalformedData(
        "sql_parser", "unexpected character '" + DescribeCharacter(ch) + "' at position " + std::to_string(offset)));
}

Expected<TokenPtr> MakeUnterminatedString(const size_t offset) {
    return tl::unexpected(
        Error::MalformedData("sql_parser", "unterminated string literal at position " + std::to_string(offset)));
}
