#include <utility>

#include "tokenizer.h"

std::string_view TokenTypeToString(const Tokens type) noexcept {
    switch (type) {
        case Tokens::kNameToken:
            return "name";
        case Tokens::kNumericLiteral:
            return "numeric_literal";
        case Tokens::kStringLiteral:
            return "string_literal";
        case Tokens::kSelect:
            return "select";
        case Tokens::kFrom:
            return "from";
        case Tokens::kCreate:
            return "create";
        case Tokens::kAs:
            return "as";
        case Tokens::kSum:
            return "sum";
        case Tokens::kCount:
            return "count";
        case Tokens::kAvg:
            return "avg";
        case Tokens::kMax:
            return "max";
        case Tokens::kMin:
            return "min";
        case Tokens::kAnd:
            return "and";
        case Tokens::kDistinct:
            return "distinct";
        case Tokens::kLength:
            return "length";
        case Tokens::kWhere:
            return "where";
        case Tokens::kBy:
            return "by";
        case Tokens::kGroup:
            return "group";
        case Tokens::kOrder:
            return "order";
        case Tokens::kLimit:
            return "limit";
        case Tokens::kPlus:
            return "plus";
        case Tokens::kMinus:
            return "minus";
        case Tokens::kAsterisk:
            return "asterisk";
        case Tokens::kOpenBracket:
            return "open_bracket";
        case Tokens::kCloseBracket:
            return "close_bracket";
        case Tokens::kComma:
            return "comma";
        case Tokens::kDot:
            return "dot";
        case Tokens::kSemicolon:
            return "semicolon";
        case Tokens::kEqual:
            return "equal";
        case Tokens::kNotEqual:
            return "not_equal";
        case Tokens::kLess:
            return "less";
        case Tokens::kLessOrEqual:
            return "less_or_equal";
        case Tokens::kGreater:
            return "greater";
        case Tokens::kGreaterOrEqual:
            return "greater_or_equal";
        case Tokens::kEndOfInput:
            return "end_of_input";
    }

    return "unknown";
}

Token::Token(std::string text, const size_t offset) : text_(std::move(text)), offset_(offset) {}

std::string_view Token::GetText() const noexcept { return text_; }

size_t Token::GetOffset() const noexcept { return offset_; }

void Command::AddArg(TokenPtr arg) { args_.push_back(std::move(arg)); }

const std::vector<TokenPtr>& Command::GetArgs() const noexcept { return args_; }

// alert: не типобезопасная вещь, лучше переписать
#define COLUMNAR_DEFINE_TOKEN_TYPE(token_class, token_value) \
    Tokens token_class::GetType() const noexcept { return Tokens::token_value; }

COLUMNAR_DEFINE_TOKEN_TYPE(TSelectToken, kSelect)
COLUMNAR_DEFINE_TOKEN_TYPE(TCreateToken, kCreate)
COLUMNAR_DEFINE_TOKEN_TYPE(TFromToken, kFrom)
COLUMNAR_DEFINE_TOKEN_TYPE(TLimitToken, kLimit)
COLUMNAR_DEFINE_TOKEN_TYPE(TOrderToken, kOrder)
COLUMNAR_DEFINE_TOKEN_TYPE(TWhereToken, kWhere)
COLUMNAR_DEFINE_TOKEN_TYPE(TGroupToken, kGroup)
COLUMNAR_DEFINE_TOKEN_TYPE(TSumToken, kSum)
COLUMNAR_DEFINE_TOKEN_TYPE(TMinToken, kMin)
COLUMNAR_DEFINE_TOKEN_TYPE(TMaxToken, kMax)
COLUMNAR_DEFINE_TOKEN_TYPE(TCountToken, kCount)
COLUMNAR_DEFINE_TOKEN_TYPE(TDistinctToken, kDistinct)
COLUMNAR_DEFINE_TOKEN_TYPE(TAvgToken, kAvg)
COLUMNAR_DEFINE_TOKEN_TYPE(TLengthToken, kLength)
COLUMNAR_DEFINE_TOKEN_TYPE(TPlusToken, kPlus)
COLUMNAR_DEFINE_TOKEN_TYPE(TMinusToken, kMinus)
COLUMNAR_DEFINE_TOKEN_TYPE(TNameToken, kNameToken)
COLUMNAR_DEFINE_TOKEN_TYPE(TNumericLiteralToken, kNumericLiteral)
COLUMNAR_DEFINE_TOKEN_TYPE(TStringLiteralToken, kStringLiteral)
COLUMNAR_DEFINE_TOKEN_TYPE(TAsToken, kAs)
COLUMNAR_DEFINE_TOKEN_TYPE(TAndToken, kAnd)
COLUMNAR_DEFINE_TOKEN_TYPE(TByToken, kBy)
COLUMNAR_DEFINE_TOKEN_TYPE(TOpenBracketToken, kOpenBracket)
COLUMNAR_DEFINE_TOKEN_TYPE(TCommaToken, kComma)
COLUMNAR_DEFINE_TOKEN_TYPE(TCloseBracketToken, kCloseBracket)
COLUMNAR_DEFINE_TOKEN_TYPE(TAsteriskToken, kAsterisk)
COLUMNAR_DEFINE_TOKEN_TYPE(TDotToken, kDot)
COLUMNAR_DEFINE_TOKEN_TYPE(TSemicolonToken, kSemicolon)
COLUMNAR_DEFINE_TOKEN_TYPE(TEqualToken, kEqual)
COLUMNAR_DEFINE_TOKEN_TYPE(TNotEqualToken, kNotEqual)
COLUMNAR_DEFINE_TOKEN_TYPE(TLessToken, kLess)
COLUMNAR_DEFINE_TOKEN_TYPE(TLessOrEqualToken, kLessOrEqual)
COLUMNAR_DEFINE_TOKEN_TYPE(TGreaterToken, kGreater)
COLUMNAR_DEFINE_TOKEN_TYPE(TGreaterOrEqualToken, kGreaterOrEqual)
COLUMNAR_DEFINE_TOKEN_TYPE(TEndOfInputToken, kEndOfInput)

#undef COLUMNAR_DEFINE_TOKEN_TYPE

std::string_view TNameToken::GetName() const noexcept { return GetText(); }
