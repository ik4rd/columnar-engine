#include "tokenizer.h"

#include <cctype>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

static bool IsIdentifierStart(const char ch) {
    const auto uch = static_cast<unsigned char>(ch);
    return std::isalpha(uch) != 0 || ch == '_';
}

static bool IsIdentifierPart(const char ch) {
    const auto uch = static_cast<unsigned char>(ch);
    return std::isalnum(uch) != 0 || ch == '_';
}

Tokenizer::Tokenizer(std::string data) : query_(std::move(data)) {}

Expected<TokenPtr> Tokenizer::GetNext() {
    while (pos_ < query_.size() && std::isspace(static_cast<unsigned char>(query_[pos_])) != 0) {
        ++pos_;
    }

    if (pos_ >= query_.size()) {
        return MakeToken(Tokens::EOI, {}, query_.size());
    }

    const char ch = query_[pos_];
    if (IsIdentifierStart(ch)) {
        const size_t start = pos_;
        ++pos_;

        while (pos_ < query_.size() && IsIdentifierPart(query_[pos_])) {
            ++pos_;
        }

        const std::string_view text = std::string_view(query_).substr(start, pos_ - start);

        return MakeToken(ResolveIdentifierType(text), std::string(text), start);
    }

    if (std::isdigit(static_cast<unsigned char>(ch)) != 0) {
        const size_t start = pos_;
        ++pos_;

        while (pos_ < query_.size() && std::isdigit(static_cast<unsigned char>(query_[pos_])) != 0) {
            ++pos_;
        }

        if (pos_ + 1 < query_.size() && query_[pos_] == '.' &&
            std::isdigit(static_cast<unsigned char>(query_[pos_ + 1])) != 0) {
            ++pos_;
            while (pos_ < query_.size() && std::isdigit(static_cast<unsigned char>(query_[pos_])) != 0) {
                ++pos_;
            }
        }

        const std::string_view text = std::string_view(query_).substr(start, pos_ - start);

        return MakeToken(Tokens::NumericLiteral, std::string(text), start);
    }

    if (ch == '\'') {
        const size_t start = pos_;
        ++pos_;

        bool terminated = false;

        while (pos_ < query_.size()) {
            if (query_[pos_] != '\'') {
                ++pos_;
                continue;
            }

            if (pos_ + 1 < query_.size() && query_[pos_ + 1] == '\'') {
                pos_ += 2;
                continue;
            }

            ++pos_;
            terminated = true;

            break;
        }

        if (!terminated) {
            return MakeUnterminatedString(start);
        }

        const std::string_view text = std::string_view(query_).substr(start, pos_ - start);

        return MakeToken(Tokens::StringLiteral, std::string(text), start);
    }

    const size_t start = pos_;
    switch (ch) {
        case '+':
            ++pos_;
            return MakeToken(Tokens::Plus, "+", start);
        case '-':
            ++pos_;
            return MakeToken(Tokens::Minus, "-", start);
        case '*':
            ++pos_;
            return MakeToken(Tokens::Asterisk, "*", start);
        case '(':
            ++pos_;
            return MakeToken(Tokens::OpenBracket, "(", start);
        case ')':
            ++pos_;
            return MakeToken(Tokens::CloseBracket, ")", start);
        case ',':
            ++pos_;
            return MakeToken(Tokens::Comma, ",", start);
        case '.':
            ++pos_;
            return MakeToken(Tokens::Dot, ".", start);
        case ';':
            ++pos_;
            return MakeToken(Tokens::Semicolon, ";", start);
        case '=':
            ++pos_;
            return MakeToken(Tokens::Equal, "=", start);
        case '<':
            ++pos_;
            if (pos_ < query_.size() && query_[pos_] == '=') {
                ++pos_;
                return MakeToken(Tokens::LessOrEqual, "<=", start);
            }
            if (pos_ < query_.size() && query_[pos_] == '>') {
                ++pos_;
                return MakeToken(Tokens::NotEqual, "<>", start);
            }
            return MakeToken(Tokens::Less, "<", start);
        case '>':
            ++pos_;
            if (pos_ < query_.size() && query_[pos_] == '=') {
                ++pos_;
                return MakeToken(Tokens::GreaterOrEqual, ">=", start);
            }
            return MakeToken(Tokens::Greater, ">", start);
        case '!':
            if (pos_ + 1 < query_.size() && query_[pos_ + 1] == '=') {
                pos_ += 2;
                return MakeToken(Tokens::NotEqual, "!=", start);
            }
            return MakeUnexpectedCharacter(ch, start);
        default:
            return MakeUnexpectedCharacter(ch, start);
    }
}

Expected<std::vector<TokenPtr>> TokenizeSql(const std::string_view input) {
    Tokenizer tokenizer{std::string(input)};

    std::vector<TokenPtr> tokens;
    tokens.reserve(input.size() / 2 + 1);

    while (true) {
        auto token = tokenizer.GetNext();
        if (!token.has_value()) {
            return tl::unexpected(token.error());
        }

        tokens.push_back(token.value());
        if (token.value()->GetType() == Tokens::EOI) {
            break;
        }
    }

    return tokens;
}
