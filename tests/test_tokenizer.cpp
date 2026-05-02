#include <memory>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "tokenizer.h"

static std::vector<Tokens> CollectTypes(const std::vector<TokenPtr>& tokens) {
    std::vector<Tokens> types;
    types.reserve(tokens.size());

    for (const TokenPtr& token : tokens) {
        types.push_back(token->GetType());
    }

    return types;
}

TEST(sql_tokenizer, tokenizes_select_query) {
    const auto result =
        TokenizeSql("SELECT DISTINCT name, COUNT(*), LENGTH(city) FROM sales GROUP BY name ORDER BY name LIMIT 10;");

    ASSERT_TRUE(result.has_value());
    const auto& tokens = result.value();

    EXPECT_EQ(
        CollectTypes(tokens),
        (std::vector{
            Tokens::Select,      Tokens::Distinct,  Tokens::NameToken,      Tokens::Comma,     Tokens::Count,
            Tokens::OpenBracket, Tokens::Asterisk,  Tokens::CloseBracket,   Tokens::Comma,     Tokens::Length,
            Tokens::OpenBracket, Tokens::NameToken, Tokens::CloseBracket,   Tokens::From,      Tokens::NameToken,
            Tokens::Group,       Tokens::By,        Tokens::NameToken,      Tokens::Order,     Tokens::By,
            Tokens::NameToken,   Tokens::Limit,     Tokens::NumericLiteral, Tokens::Semicolon, Tokens::EOI,
        }));

    ASSERT_EQ(tokens.size(), 25U);
    EXPECT_NE(std::dynamic_pointer_cast<TSelectToken>(tokens[0]), nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<TDistinctToken>(tokens[1]), nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<TNameToken>(tokens[2]), nullptr);
    EXPECT_EQ(tokens[2]->GetText(), "name");
    EXPECT_EQ(tokens[14]->GetText(), "sales");
    EXPECT_EQ(tokens[22]->GetText(), "10");
    EXPECT_EQ(tokens[24]->GetOffset(), std::string_view("SELECT DISTINCT name, COUNT(*), LENGTH(city) FROM sales "
                                                        "GROUP BY name ORDER BY name LIMIT 10;")
                                           .size());
}

TEST(sql_tokenizer, tokenizes_create_with_literals_and_case_insensitive_keywords) {
    const auto result = TokenizeSql("create report as SeLeCt 'abc''def', total_sum - 10.5 FROM source_table");

    ASSERT_TRUE(result.has_value());
    const auto& tokens = result.value();

    EXPECT_EQ(CollectTypes(tokens), (std::vector{
                                        Tokens::Create,
                                        Tokens::NameToken,
                                        Tokens::As,
                                        Tokens::Select,
                                        Tokens::StringLiteral,
                                        Tokens::Comma,
                                        Tokens::NameToken,
                                        Tokens::Minus,
                                        Tokens::NumericLiteral,
                                        Tokens::From,
                                        Tokens::NameToken,
                                        Tokens::EOI,
                                    }));

    ASSERT_EQ(tokens.size(), 12U);
    EXPECT_NE(std::dynamic_pointer_cast<TCreateToken>(tokens[0]), nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<TStringLiteralToken>(tokens[4]), nullptr);
    EXPECT_EQ(tokens[1]->GetText(), "report");
    EXPECT_EQ(tokens[4]->GetText(), "'abc''def'");
    EXPECT_EQ(tokens[8]->GetText(), "10.5");
    EXPECT_EQ(tokens[10]->GetText(), "source_table");
}

TEST(sql_tokenizer, tokenizer_returns_tokens_one_by_one) {
    Tokenizer tokenizer("WHERE value >= 10 AND value <> 20 AND value != 30");

    std::vector<Tokens> types;
    while (true) {
        auto token = tokenizer.GetNext();
        ASSERT_TRUE(token.has_value());
        types.push_back(token.value()->GetType());

        if (token.value()->GetType() == Tokens::EOI) {
            break;
        }
    }

    EXPECT_EQ(types, (std::vector{
                         Tokens::Where,
                         Tokens::NameToken,
                         Tokens::GreaterOrEqual,
                         Tokens::NumericLiteral,
                         Tokens::And,
                         Tokens::NameToken,
                         Tokens::NotEqual,
                         Tokens::NumericLiteral,
                         Tokens::And,
                         Tokens::NameToken,
                         Tokens::NotEqual,
                         Tokens::NumericLiteral,
                         Tokens::EOI,
                     }));
}

TEST(sql_tokenizer, rejects_invalid_input) {
    const auto unterminated = TokenizeSql("SELECT 'oops");
    ASSERT_FALSE(unterminated.has_value());
    EXPECT_EQ(unterminated.error().GetCode(), Error::Code::InvalidData);

    const auto invalid_character = TokenizeSql("SELECT @oops");
    ASSERT_FALSE(invalid_character.has_value());
    EXPECT_EQ(invalid_character.error().GetCode(), Error::Code::InvalidData);
}
