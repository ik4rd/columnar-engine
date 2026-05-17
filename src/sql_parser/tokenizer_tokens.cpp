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

std::string_view TNameToken::GetName() const noexcept { return GetText(); }
