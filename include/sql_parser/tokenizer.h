#pragma once

#include <cstddef>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "error.h"

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

    EOI,
};

std::string_view TokenTypeToString(Tokens type) noexcept;

template <typename T>
using Expected = std::expected<T, Error>;

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

class TSelectToken : public Command {
   public:
    using Command::Command;
    Tokens GetType() const noexcept override;
};

class TCreateToken : public Command {
   public:
    using Command::Command;
    Tokens GetType() const noexcept override;
};

class TFromToken : public Command {
   public:
    using Command::Command;
    Tokens GetType() const noexcept override;
};

class TLimitToken : public Command {
   public:
    using Command::Command;
    Tokens GetType() const noexcept override;
};

class TOrderToken : public Command {
   public:
    using Command::Command;
    Tokens GetType() const noexcept override;
};

class TWhereToken : public Command {
   public:
    using Command::Command;
    Tokens GetType() const noexcept override;
};

class TGroupToken : public Command {
   public:
    using Command::Command;
    Tokens GetType() const noexcept override;
};

class TSumToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TMinToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TMaxToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TCountToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TDistinctToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TAvgToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TLengthToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TPlusToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TMinusToken : public IOperatorToken {
   public:
    using IOperatorToken::IOperatorToken;
    Tokens GetType() const noexcept override;
};

class TNameToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;

    std::string_view GetName() const noexcept;
};

class TNumericLiteralToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TStringLiteralToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TAsToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TAndToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TByToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TOpenBracketToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TCommaToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TCloseBracketToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TAsteriskToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TDotToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TSemicolonToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TEqualToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TNotEqualToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TLessToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TLessOrEqualToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TGreaterToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TGreaterOrEqualToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

class TEndOfInputToken : public Token {
   public:
    using Token::Token;
    Tokens GetType() const noexcept override;
};

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
