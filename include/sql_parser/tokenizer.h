#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/error.h"
#include "tl/expected.hpp"

enum class Tokens {
    NameToken,
    NumericLiteral,
    StringLiteral,

    Select,
    From,
    Create,
    As,
    Sum,
    Count,
    Avg,
    Max,
    Min,
    And,
    Distinct,
    Length,
    Where,
    By,
    Group,
    Order,
    Limit,

    Plus,
    Minus,
    Asterisk,
    OpenBracket,
    CloseBracket,
    Comma,
    Dot,
    Semicolon,

    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,

    EndOfInput,
};

std::string_view TokenTypeToString(Tokens type) noexcept;

template <typename T>
using Expected = tl::expected<T, Error>;

class Token {
   public:
    Token(std::string text, size_t offset);
    virtual ~Token() = default;

    virtual Tokens GetType() const noexcept = 0;

    std::string_view GetText() const noexcept;
    size_t GetOffset() const noexcept;

   private:
    std::string text_;
    size_t offset_ = 0;
};

using TokenPtr = std::shared_ptr<Token>;

class Command : public Token {
   public:
    using Token::Token;
    ~Command() override = default;

    void AddArg(TokenPtr arg);
    const std::vector<TokenPtr>& GetArgs() const noexcept;

   protected:
    std::vector<TokenPtr> args_;
};

class IOperatorToken : public Command {
   public:
    using Command::Command;
    ~IOperatorToken() override = default;
};

template <typename BaseToken, Tokens TokenType>
class TypedToken : public BaseToken {
   public:
    using BaseToken::BaseToken;

    Tokens GetType() const noexcept override { return TokenType; }
};

using TSelectToken = TypedToken<Command, Tokens::Select>;
using TCreateToken = TypedToken<Command, Tokens::Create>;
using TFromToken = TypedToken<Command, Tokens::From>;
using TLimitToken = TypedToken<Command, Tokens::Limit>;
using TOrderToken = TypedToken<Command, Tokens::Order>;
using TWhereToken = TypedToken<Command, Tokens::Where>;
using TGroupToken = TypedToken<Command, Tokens::Group>;

using TSumToken = TypedToken<IOperatorToken, Tokens::Sum>;
using TMinToken = TypedToken<IOperatorToken, Tokens::Min>;
using TMaxToken = TypedToken<IOperatorToken, Tokens::Max>;
using TCountToken = TypedToken<IOperatorToken, Tokens::Count>;
using TDistinctToken = TypedToken<IOperatorToken, Tokens::Distinct>;
using TAvgToken = TypedToken<IOperatorToken, Tokens::Avg>;
using TLengthToken = TypedToken<IOperatorToken, Tokens::Length>;
using TPlusToken = TypedToken<IOperatorToken, Tokens::Plus>;
using TMinusToken = TypedToken<IOperatorToken, Tokens::Minus>;

class TNameToken : public TypedToken<Token, Tokens::NameToken> {
   public:
    using TypedToken::TypedToken;

    std::string_view GetName() const noexcept;
};

using TNumericLiteralToken = TypedToken<Token, Tokens::NumericLiteral>;
using TStringLiteralToken = TypedToken<Token, Tokens::StringLiteral>;
using TAsToken = TypedToken<Token, Tokens::As>;
using TAndToken = TypedToken<Token, Tokens::And>;
using TByToken = TypedToken<Token, Tokens::By>;
using TOpenBracketToken = TypedToken<Token, Tokens::OpenBracket>;
using TCommaToken = TypedToken<Token, Tokens::Comma>;
using TCloseBracketToken = TypedToken<Token, Tokens::CloseBracket>;
using TAsteriskToken = TypedToken<Token, Tokens::Asterisk>;
using TDotToken = TypedToken<Token, Tokens::Dot>;
using TSemicolonToken = TypedToken<Token, Tokens::Semicolon>;
using TEqualToken = TypedToken<Token, Tokens::Equal>;
using TNotEqualToken = TypedToken<Token, Tokens::NotEqual>;
using TLessToken = TypedToken<Token, Tokens::Less>;
using TLessOrEqualToken = TypedToken<Token, Tokens::LessOrEqual>;
using TGreaterToken = TypedToken<Token, Tokens::Greater>;
using TGreaterOrEqualToken = TypedToken<Token, Tokens::GreaterOrEqual>;
using TEndOfInputToken = TypedToken<Token, Tokens::EndOfInput>;

class Tokenizer {
   public:
    explicit Tokenizer(std::string data);

    Expected<TokenPtr> GetNext();

   private:
    std::string query_;
    size_t pos_ = 0;
};

Expected<std::vector<TokenPtr>> TokenizeSql(std::string_view input);

Tokens ResolveIdentifierType(std::string_view text);
TokenPtr MakeToken(Tokens type, std::string text, size_t offset);
Expected<TokenPtr> MakeUnexpectedCharacter(char ch, size_t offset);
Expected<TokenPtr> MakeUnterminatedString(size_t offset);
