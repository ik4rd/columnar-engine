#include <cctype>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "tokenizer.h"

static const std::unordered_map<std::string, Tokens> kKeywords = {
    {"AS", Tokens::kAs},       {"AND", Tokens::kAnd},       {"AVG", Tokens::kAvg},           {"BY", Tokens::kBy},
    {"COUNT", Tokens::kCount}, {"CREATE", Tokens::kCreate}, {"DISTINCT", Tokens::kDistinct}, {"FROM", Tokens::kFrom},
    {"GROUP", Tokens::kGroup}, {"LENGTH", Tokens::kLength}, {"LIMIT", Tokens::kLimit},       {"MAX", Tokens::kMax},
    {"MIN", Tokens::kMin},     {"ORDER", Tokens::kOrder},   {"SELECT", Tokens::kSelect},     {"SUM", Tokens::kSum},
    {"WHERE", Tokens::kWhere},
};

static std::string ToUpperAscii(const std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (const char ch : input) {
        output.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
    }
    return output;
}

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
    const auto it = kKeywords.find(upper);
    if (it != kKeywords.end()) {
        return it->second;
    }

    return Tokens::kNameToken;
}

TokenPtr MakeToken(const Tokens type, std::string text, const size_t offset) {
    switch (type) {
        case Tokens::kNameToken:
            return std::make_shared<TNameToken>(std::move(text), offset);
        case Tokens::kNumericLiteral:
            return std::make_shared<TNumericLiteralToken>(std::move(text), offset);
        case Tokens::kStringLiteral:
            return std::make_shared<TStringLiteralToken>(std::move(text), offset);
        case Tokens::kSelect:
            return std::make_shared<TSelectToken>(std::move(text), offset);
        case Tokens::kFrom:
            return std::make_shared<TFromToken>(std::move(text), offset);
        case Tokens::kCreate:
            return std::make_shared<TCreateToken>(std::move(text), offset);
        case Tokens::kAs:
            return std::make_shared<TAsToken>(std::move(text), offset);
        case Tokens::kSum:
            return std::make_shared<TSumToken>(std::move(text), offset);
        case Tokens::kCount:
            return std::make_shared<TCountToken>(std::move(text), offset);
        case Tokens::kAvg:
            return std::make_shared<TAvgToken>(std::move(text), offset);
        case Tokens::kMax:
            return std::make_shared<TMaxToken>(std::move(text), offset);
        case Tokens::kMin:
            return std::make_shared<TMinToken>(std::move(text), offset);
        case Tokens::kAnd:
            return std::make_shared<TAndToken>(std::move(text), offset);
        case Tokens::kDistinct:
            return std::make_shared<TDistinctToken>(std::move(text), offset);
        case Tokens::kLength:
            return std::make_shared<TLengthToken>(std::move(text), offset);
        case Tokens::kWhere:
            return std::make_shared<TWhereToken>(std::move(text), offset);
        case Tokens::kBy:
            return std::make_shared<TByToken>(std::move(text), offset);
        case Tokens::kGroup:
            return std::make_shared<TGroupToken>(std::move(text), offset);
        case Tokens::kOrder:
            return std::make_shared<TOrderToken>(std::move(text), offset);
        case Tokens::kLimit:
            return std::make_shared<TLimitToken>(std::move(text), offset);
        case Tokens::kPlus:
            return std::make_shared<TPlusToken>(std::move(text), offset);
        case Tokens::kMinus:
            return std::make_shared<TMinusToken>(std::move(text), offset);
        case Tokens::kAsterisk:
            return std::make_shared<TAsteriskToken>(std::move(text), offset);
        case Tokens::kOpenBracket:
            return std::make_shared<TOpenBracketToken>(std::move(text), offset);
        case Tokens::kCloseBracket:
            return std::make_shared<TCloseBracketToken>(std::move(text), offset);
        case Tokens::kComma:
            return std::make_shared<TCommaToken>(std::move(text), offset);
        case Tokens::kDot:
            return std::make_shared<TDotToken>(std::move(text), offset);
        case Tokens::kSemicolon:
            return std::make_shared<TSemicolonToken>(std::move(text), offset);
        case Tokens::kEqual:
            return std::make_shared<TEqualToken>(std::move(text), offset);
        case Tokens::kNotEqual:
            return std::make_shared<TNotEqualToken>(std::move(text), offset);
        case Tokens::kLess:
            return std::make_shared<TLessToken>(std::move(text), offset);
        case Tokens::kLessOrEqual:
            return std::make_shared<TLessOrEqualToken>(std::move(text), offset);
        case Tokens::kGreater:
            return std::make_shared<TGreaterToken>(std::move(text), offset);
        case Tokens::kGreaterOrEqual:
            return std::make_shared<TGreaterOrEqualToken>(std::move(text), offset);
        case Tokens::kEndOfInput:
            return std::make_shared<TEndOfInputToken>(std::move(text), offset);
    }

    return nullptr;
}

Expected<TokenPtr> MakeUnexpectedCharacter(const char ch, const size_t offset) {
    return std::unexpected(Error::InvalidData(
        "sql_tokenizer", "unexpected character '" + DescribeCharacter(ch) + "' at position " + std::to_string(offset)));
}

Expected<TokenPtr> MakeUnterminatedString(const size_t offset) {
    return std::unexpected(
        Error::InvalidData("sql_tokenizer", "unterminated string literal at position " + std::to_string(offset)));
}
