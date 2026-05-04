#include <utility>

#include "sql_parser/tokenizer.h"

std::string_view TokenTypeToString(const Tokens type) noexcept {
    switch (type) {
        case Tokens::NameToken:
            return "name";
        case Tokens::NumericLiteral:
            return "numeric_literal";
        case Tokens::StringLiteral:
            return "string_literal";
        case Tokens::Select:
            return "select";
        case Tokens::From:
            return "from";
        case Tokens::Create:
            return "create";
        case Tokens::As:
            return "as";
        case Tokens::Sum:
            return "sum";
        case Tokens::Count:
            return "count";
        case Tokens::Avg:
            return "avg";
        case Tokens::Max:
            return "max";
        case Tokens::Min:
            return "min";
        case Tokens::And:
            return "and";
        case Tokens::Distinct:
            return "distinct";
        case Tokens::Length:
            return "length";
        case Tokens::Where:
            return "where";
        case Tokens::By:
            return "by";
        case Tokens::Group:
            return "group";
        case Tokens::Order:
            return "order";
        case Tokens::Limit:
            return "limit";
        case Tokens::Plus:
            return "plus";
        case Tokens::Minus:
            return "minus";
        case Tokens::Asterisk:
            return "asterisk";
        case Tokens::OpenBracket:
            return "open_bracket";
        case Tokens::CloseBracket:
            return "close_bracket";
        case Tokens::Comma:
            return "comma";
        case Tokens::Dot:
            return "dot";
        case Tokens::Semicolon:
            return "semicolon";
        case Tokens::Equal:
            return "equal";
        case Tokens::NotEqual:
            return "not_equal";
        case Tokens::Less:
            return "less";
        case Tokens::LessOrEqual:
            return "less_or_equal";
        case Tokens::Greater:
            return "greater";
        case Tokens::GreaterOrEqual:
            return "greater_or_equal";
        case Tokens::EndOfInput:
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

COLUMNAR_DEFINE_TOKEN_TYPE(TSelectToken, Select)
COLUMNAR_DEFINE_TOKEN_TYPE(TCreateToken, Create)
COLUMNAR_DEFINE_TOKEN_TYPE(TFromToken, From)
COLUMNAR_DEFINE_TOKEN_TYPE(TLimitToken, Limit)
COLUMNAR_DEFINE_TOKEN_TYPE(TOrderToken, Order)
COLUMNAR_DEFINE_TOKEN_TYPE(TWhereToken, Where)
COLUMNAR_DEFINE_TOKEN_TYPE(TGroupToken, Group)
COLUMNAR_DEFINE_TOKEN_TYPE(TSumToken, Sum)
COLUMNAR_DEFINE_TOKEN_TYPE(TMinToken, Min)
COLUMNAR_DEFINE_TOKEN_TYPE(TMaxToken, Max)
COLUMNAR_DEFINE_TOKEN_TYPE(TCountToken, Count)
COLUMNAR_DEFINE_TOKEN_TYPE(TDistinctToken, Distinct)
COLUMNAR_DEFINE_TOKEN_TYPE(TAvgToken, Avg)
COLUMNAR_DEFINE_TOKEN_TYPE(TLengthToken, Length)
COLUMNAR_DEFINE_TOKEN_TYPE(TPlusToken, Plus)
COLUMNAR_DEFINE_TOKEN_TYPE(TMinusToken, Minus)
COLUMNAR_DEFINE_TOKEN_TYPE(TNameToken, NameToken)
COLUMNAR_DEFINE_TOKEN_TYPE(TNumericLiteralToken, NumericLiteral)
COLUMNAR_DEFINE_TOKEN_TYPE(TStringLiteralToken, StringLiteral)
COLUMNAR_DEFINE_TOKEN_TYPE(TAsToken, As)
COLUMNAR_DEFINE_TOKEN_TYPE(TAndToken, And)
COLUMNAR_DEFINE_TOKEN_TYPE(TByToken, By)
COLUMNAR_DEFINE_TOKEN_TYPE(TOpenBracketToken, OpenBracket)
COLUMNAR_DEFINE_TOKEN_TYPE(TCommaToken, Comma)
COLUMNAR_DEFINE_TOKEN_TYPE(TCloseBracketToken, CloseBracket)
COLUMNAR_DEFINE_TOKEN_TYPE(TAsteriskToken, Asterisk)
COLUMNAR_DEFINE_TOKEN_TYPE(TDotToken, Dot)
COLUMNAR_DEFINE_TOKEN_TYPE(TSemicolonToken, Semicolon)
COLUMNAR_DEFINE_TOKEN_TYPE(TEqualToken, Equal)
COLUMNAR_DEFINE_TOKEN_TYPE(TNotEqualToken, NotEqual)
COLUMNAR_DEFINE_TOKEN_TYPE(TLessToken, Less)
COLUMNAR_DEFINE_TOKEN_TYPE(TLessOrEqualToken, LessOrEqual)
COLUMNAR_DEFINE_TOKEN_TYPE(TGreaterToken, Greater)
COLUMNAR_DEFINE_TOKEN_TYPE(TGreaterOrEqualToken, GreaterOrEqual)
COLUMNAR_DEFINE_TOKEN_TYPE(TEndOfInputToken, EndOfInput)

#undef COLUMNAR_DEFINE_TOKEN_TYPE

std::string_view TNameToken::GetName() const noexcept { return GetText(); }
