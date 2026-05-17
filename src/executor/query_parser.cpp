#include "executor/query_parser.h"

#include <charconv>
#include <utility>
#include <vector>

#include "common/ascii.h"
#include "common/error.h"
#include "sql_parser/tokenizer.h"

class TokenCursor {
   public:
    explicit TokenCursor(std::vector<TokenPtr> tokens) : tokens_(std::move(tokens)) {}

    const Token& Peek(const size_t lookahead = 0) const {
        if (pos_ + lookahead >= tokens_.size()) {
            throw Error::InvalidArgument("executor", "unexpected end of query");
        }
        return *tokens_[pos_ + lookahead];
    }

    bool Match(const Tokens type, const size_t lookahead = 0) const {
        return pos_ + lookahead < tokens_.size() && tokens_[pos_ + lookahead]->GetType() == type;
    }

    const Token& Consume(const Tokens type, const std::string_view message) {
        if (!Match(type)) {
            throw Error::InvalidArgument("executor", std::string(message));
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
            throw Error::InvalidArgument("executor", std::string(message));
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
            case Tokens::EndOfInput:
                throw Error::InvalidArgument("executor", std::string(message));
            default:
                break;
        }

        return *tokens_[pos_++];
    }

   private:
    std::vector<TokenPtr> tokens_;
    size_t pos_ = 0;
};

static bool IsAggregateName(const std::string_view name) {
    const std::string upper = ToUpperAscii(name);
    return upper == "COUNT" || upper == "SUM" || upper == "AVG" || upper == "MIN" || upper == "MAX";
}

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
            throw Error::InvalidArgument("executor", "expected comparison operator");
    }
}

static std::string FormatColumnRef(const ColumnRef& column) {
    if (column.qualifier.empty()) {
        return column.name;
    }
    return column.qualifier + "." + column.name;
}

static ExprPtr MakeExpr(const ExprKind kind) {
    auto expr = std::make_shared<ExprSpec>();
    expr->kind = kind;
    return expr;
}

static std::string FormatExpression(const ExprSpec& expr);

static std::string FormatFunctionCall(const ExprSpec& expr) {
    std::string output = ToUpperAscii(expr.function_name);

    output.push_back('(');
    for (size_t i = 0; i < expr.arguments.size(); ++i) {
        if (i > 0) {
            output += ", ";
        }
        output += FormatExpression(*expr.arguments[i]);
    }
    output.push_back(')');

    return output;
}

static std::string FormatExpression(const ExprSpec& expr) {
    switch (expr.kind) {
        case ExprKind::Column:
            return FormatColumnRef(expr.column);
        case ExprKind::Literal:
            return expr.literal.text;
        case ExprKind::Star:
            return "*";
        case ExprKind::Binary:
            return FormatExpression(*expr.left) + (expr.binary_op == BinaryOp::Add ? " + " : " - ") +
                   FormatExpression(*expr.right);
        case ExprKind::Function:
            return FormatFunctionCall(expr);
        case ExprKind::Case:
            return "CASE";
    }

    return {};
}

static ExprPtr ParseExpression(TokenCursor& cursor);
static PredicatePtr ParsePredicate(TokenCursor& cursor);

static ExprPtr ParseFunction(TokenCursor& cursor, const std::string& function_name) {
    auto expr = MakeExpr(ExprKind::Function);
    expr->function_name = ToUpperAscii(function_name);

    cursor.Consume(Tokens::OpenBracket, "expected '(' after function name");

    if (ToUpperAscii(function_name) == "EXTRACT") {
        auto part = MakeExpr(ExprKind::Literal);
        part->literal = QueryLiteral{std::string(cursor.Consume(Tokens::NameToken, "expected EXTRACT part").GetText()),
                                     LiteralKind::Identifier};

        cursor.Consume(Tokens::From, "expected FROM in EXTRACT");
        expr->arguments.push_back(std::move(part));
        expr->arguments.push_back(ParseExpression(cursor));
        cursor.Consume(Tokens::CloseBracket, "expected ')' after EXTRACT");
        expr->output_name = FormatFunctionCall(*expr);

        return expr;
    }

    if (!cursor.Match(Tokens::CloseBracket)) {
        expr->arguments.push_back(ParseExpression(cursor));
        while (cursor.TryConsume(Tokens::Comma)) {
            expr->arguments.push_back(ParseExpression(cursor));
        }
    }

    cursor.Consume(Tokens::CloseBracket, "expected ')' after function arguments");
    expr->output_name = FormatFunctionCall(*expr);

    return expr;
}

static ExprPtr ParseCaseExpression(TokenCursor& cursor) {
    cursor.Consume(Tokens::Case, "expected CASE");
    cursor.Consume(Tokens::When, "expected WHEN after CASE");

    auto expr = MakeExpr(ExprKind::Case);
    expr->case_spec.condition = ParsePredicate(cursor);
    cursor.Consume(Tokens::Then, "expected THEN in CASE");
    expr->case_spec.then_expr = ParseExpression(cursor);
    cursor.Consume(Tokens::Else, "expected ELSE in CASE");
    expr->case_spec.else_expr = ParseExpression(cursor);
    cursor.Consume(Tokens::End, "expected END in CASE");
    expr->output_name = "CASE";

    return expr;
}

static ExprPtr ParsePrimary(TokenCursor& cursor) {
    if (cursor.TryConsume(Tokens::Minus)) {
        auto zero = MakeExpr(ExprKind::Literal);
        zero->literal = QueryLiteral{"0", LiteralKind::Numeric};
        auto expr = MakeExpr(ExprKind::Binary);
        expr->binary_op = BinaryOp::Subtract;
        expr->left = std::move(zero);
        expr->right = ParsePrimary(cursor);
        expr->output_name = FormatExpression(*expr);
        return expr;
    }

    if (cursor.TryConsume(Tokens::Asterisk)) {
        auto expr = MakeExpr(ExprKind::Star);
        expr->output_name = "*";
        return expr;
    }

    if (cursor.Match(Tokens::Case)) {
        return ParseCaseExpression(cursor);
    }

    if (cursor.TryConsume(Tokens::OpenBracket)) {
        if (cursor.Match(Tokens::Select)) {
            throw Error::Unsupported("executor", "subqueries are not supported");
        }
        auto expr = ParseExpression(cursor);
        cursor.Consume(Tokens::CloseBracket, "expected ')' after expression");
        return expr;
    }

    if (cursor.Match(Tokens::NumericLiteral)) {
        auto expr = MakeExpr(ExprKind::Literal);
        expr->literal =
            QueryLiteral{std::string(cursor.Consume(Tokens::NumericLiteral, "expected numeric literal").GetText()),
                         LiteralKind::Numeric};
        expr->output_name = expr->literal.text;
        return expr;
    }

    if (cursor.Match(Tokens::StringLiteral)) {
        auto expr = MakeExpr(ExprKind::Literal);
        expr->literal =
            QueryLiteral{std::string(cursor.Consume(Tokens::StringLiteral, "expected string literal").GetText()),
                         LiteralKind::String};
        expr->output_name = expr->literal.text;
        return expr;
    }

    if (cursor.Match(Tokens::NameToken) || cursor.Match(Tokens::Length) || cursor.Match(Tokens::Count) ||
        cursor.Match(Tokens::Sum) || cursor.Match(Tokens::Avg) || cursor.Match(Tokens::Min) ||
        cursor.Match(Tokens::Max)) {
        const Token& token = cursor.ConsumeFunctionToken("expected expression");
        const auto name = std::string(token.GetText());

        if (cursor.Match(Tokens::OpenBracket)) {
            return ParseFunction(cursor, name);
        }

        auto expr = MakeExpr(ExprKind::Column);
        expr->column = ColumnRef{.qualifier = {}, .name = name};

        if (cursor.TryConsume(Tokens::Dot)) {
            expr->column.qualifier = std::move(expr->column.name);
            expr->column.name =
                std::string(cursor.Consume(Tokens::NameToken, "expected column name after '.'").GetText());
        }

        expr->output_name = expr->column.name;

        return expr;
    }

    throw Error::InvalidArgument("executor", "expected expression");
}

static ExprPtr ParseExpression(TokenCursor& cursor) {
    auto expr = ParsePrimary(cursor);

    while (cursor.Match(Tokens::Plus) || cursor.Match(Tokens::Minus)) {
        const bool add = cursor.TryConsume(Tokens::Plus);
        if (!add) {
            cursor.Consume(Tokens::Minus, "expected '-'");
        }

        auto binary = MakeExpr(ExprKind::Binary);
        binary->binary_op = add ? BinaryOp::Add : BinaryOp::Subtract;
        binary->left = std::move(expr);
        binary->right = ParsePrimary(cursor);
        binary->output_name = FormatExpression(*binary);
        expr = std::move(binary);
    }

    return expr;
}

static PredicatePtr MakePredicate(const PredicateKind kind) {
    auto predicate = std::make_shared<PredicateSpec>();
    predicate->kind = kind;
    return predicate;
}

static PredicatePtr ParsePredicateAtom(TokenCursor& cursor) {
    if (cursor.TryConsume(Tokens::OpenBracket)) {
        auto nested = ParsePredicate(cursor);
        cursor.Consume(Tokens::CloseBracket, "expected ')' after predicate");
        return nested;
    }

    auto left = ParseExpression(cursor);

    if (cursor.TryConsume(Tokens::Not)) {
        if (cursor.TryConsume(Tokens::Like)) {
            auto predicate = MakePredicate(PredicateKind::NotLike);
            predicate->left = std::move(left);
            predicate->right = ParseExpression(cursor);
            return predicate;
        }
        throw Error::InvalidArgument("executor", "expected LIKE after NOT");
    }

    if (cursor.TryConsume(Tokens::Like)) {
        auto predicate = MakePredicate(PredicateKind::Like);
        predicate->left = std::move(left);
        predicate->right = ParseExpression(cursor);
        return predicate;
    }

    if (cursor.TryConsume(Tokens::In)) {
        auto predicate = MakePredicate(PredicateKind::In);
        predicate->left = std::move(left);
        cursor.Consume(Tokens::OpenBracket, "expected '(' after IN");
        predicate->values.push_back(ParseExpression(cursor));

        while (cursor.TryConsume(Tokens::Comma)) {
            predicate->values.push_back(ParseExpression(cursor));
        }

        cursor.Consume(Tokens::CloseBracket, "expected ')' after IN list");

        return predicate;
    }

    const Tokens comparison_token = cursor.Peek().GetType();
    auto predicate = MakePredicate(PredicateKind::Comparison);
    predicate->left = std::move(left);
    predicate->comparison = ComparisonKindFromToken(comparison_token);
    cursor.Consume(comparison_token, "expected comparison operator");
    predicate->right = ParseExpression(cursor);

    return predicate;
}

static PredicatePtr ParsePredicate(TokenCursor& cursor) {
    auto predicate = ParsePredicateAtom(cursor);

    while (cursor.TryConsume(Tokens::And)) {
        auto and_predicate = MakePredicate(PredicateKind::And);
        and_predicate->lhs = std::move(predicate);
        and_predicate->rhs = ParsePredicateAtom(cursor);
        predicate = std::move(and_predicate);
    }

    return predicate;
}

static std::optional<size_t> ParseOptionalSize(TokenCursor& cursor, const Tokens token, const std::string_view name) {
    if (!cursor.TryConsume(token)) {
        return std::nullopt;
    }

    const Token& value_token =
        cursor.Consume(Tokens::NumericLiteral, "expected numeric literal after " + std::string(name));
    size_t value = 0;
    const std::string_view text = value_token.GetText();

    const char* begin = text.data();
    const char* end = begin + text.size();
    if (const auto [ptr, ec] = std::from_chars(begin, end, value); ec != std::errc() || ptr != end) {
        throw Error::InvalidArgument("executor", "invalid " + std::string(name) + " value");
    }

    return value;
}

static AggSpec ParseAggregate(TokenCursor& cursor, const std::string& function_name) {
    AggSpec spec;
    spec.function_name = ToUpperAscii(function_name);
    cursor.Consume(Tokens::OpenBracket, "expected '(' after aggregate function");

    if (cursor.TryConsume(Tokens::Asterisk)) {
        spec.argument_kind = AggArgumentKind::Star;
    } else {
        spec.distinct = cursor.TryConsume(Tokens::Distinct);
        spec.argument = ParseExpression(cursor);
        if (spec.argument->kind == ExprKind::Column) {
            spec.column = spec.argument->column;
        }
    }

    cursor.Consume(Tokens::CloseBracket, "expected ')' after aggregate arguments");

    if (spec.argument_kind == AggArgumentKind::Star) {
        spec.output_name = spec.function_name + "(*)";
    } else {
        spec.output_name = spec.function_name + "(";
        if (spec.distinct) {
            spec.output_name += "DISTINCT ";
        }
        spec.output_name += FormatExpression(*spec.argument);
        spec.output_name += ")";
    }

    return spec;
}

static SelectItemSpec ParseSelectItem(TokenCursor& cursor) {
    if ((cursor.Match(Tokens::NameToken) || cursor.Match(Tokens::Count) || cursor.Match(Tokens::Sum) ||
         cursor.Match(Tokens::Avg) || cursor.Match(Tokens::Min) || cursor.Match(Tokens::Max)) &&
        cursor.Match(Tokens::OpenBracket, 1) && IsAggregateName(cursor.Peek().GetText())) {
        const auto function_name = std::string(cursor.ConsumeFunctionToken("expected aggregate").GetText());

        SelectItemSpec item{
            .kind = SelectItemKind::Aggregate,
            .column = {},
            .expression = {},
            .aggregate = ParseAggregate(cursor, function_name),
            .output_name = {},
        };
        item.output_name = item.aggregate.output_name;

        if (cursor.TryConsume(Tokens::As)) {
            item.output_name = std::string(cursor.Consume(Tokens::NameToken, "expected alias after AS").GetText());
        } else if (cursor.Match(Tokens::NameToken)) {
            item.output_name = std::string(cursor.Consume(Tokens::NameToken, "expected alias").GetText());
        }

        return item;
    }

    const auto expr = ParseExpression(cursor);

    SelectItemSpec item{
        .kind = SelectItemKind::GroupKey,
        .column = expr->kind == ExprKind::Column ? expr->column : ColumnRef{},
        .expression = expr,
        .aggregate = {},
        .output_name = expr->output_name.empty() ? FormatExpression(*expr) : expr->output_name,
    };

    if (cursor.TryConsume(Tokens::As)) {
        item.output_name = std::string(cursor.Consume(Tokens::NameToken, "expected alias after AS").GetText());
    } else if (cursor.Match(Tokens::NameToken)) {
        item.output_name = std::string(cursor.Consume(Tokens::NameToken, "expected alias").GetText());
    }

    return item;
}

static OrderBySpec ParseOrderByItem(TokenCursor& cursor) {
    OrderBySpec order;

    if ((cursor.Match(Tokens::NameToken) || cursor.Match(Tokens::Count) || cursor.Match(Tokens::Sum) ||
         cursor.Match(Tokens::Avg) || cursor.Match(Tokens::Min) || cursor.Match(Tokens::Max)) &&
        cursor.Match(Tokens::OpenBracket, 1) && IsAggregateName(cursor.Peek().GetText())) {
        const auto function_name = std::string(cursor.ConsumeFunctionToken("expected aggregate").GetText());
        order.item = SelectItemSpec{
            .kind = SelectItemKind::Aggregate,
            .column = {},
            .expression = {},
            .aggregate = ParseAggregate(cursor, function_name),
            .output_name = {},
        };
        order.item.output_name = order.item.aggregate.output_name;
    } else {
        const auto expr = ParseExpression(cursor);
        order.item = SelectItemSpec{
            .kind = SelectItemKind::GroupKey,
            .column = expr->kind == ExprKind::Column ? expr->column : ColumnRef{},
            .expression = expr,
            .aggregate = {},
            .output_name = expr->output_name.empty() ? FormatExpression(*expr) : expr->output_name,
        };
    }

    if (cursor.Match(Tokens::NameToken)) {
        const std::string direction = ToUpperAscii(cursor.Peek().GetText());
        if (direction == "DESC") {
            cursor.Consume(Tokens::NameToken, "expected DESC");
            order.descending = true;
        } else if (direction == "ASC") {
            cursor.Consume(Tokens::NameToken, "expected ASC");
        }
    }

    return order;
}

Query ParseQuery(const std::string_view query) {
    auto tokenized = TokenizeSql(query);
    if (!tokenized.has_value()) {
        throw tokenized.error();
    }

    TokenCursor cursor(std::move(tokenized.value()));

    Query query_ast;
    cursor.Consume(Tokens::Select, "expected SELECT");

    query_ast.select_items.push_back(ParseSelectItem(cursor));
    while (cursor.TryConsume(Tokens::Comma)) {
        query_ast.select_items.push_back(ParseSelectItem(cursor));
    }

    cursor.Consume(Tokens::From, "expected FROM");
    query_ast.table_name = std::string(cursor.Consume(Tokens::NameToken, "expected table name").GetText());

    if (cursor.TryConsume(Tokens::As)) {
        query_ast.table_alias = std::string(cursor.Consume(Tokens::NameToken, "expected alias after AS").GetText());
    } else if (cursor.Match(Tokens::NameToken)) {
        query_ast.table_alias = std::string(cursor.Consume(Tokens::NameToken, "expected table alias").GetText());
    }

    if (cursor.TryConsume(Tokens::Where)) {
        query_ast.filter = ParsePredicate(cursor);
    }

    if (cursor.TryConsume(Tokens::Group)) {
        cursor.Consume(Tokens::By, "expected BY after GROUP");
        query_ast.group_by.push_back(ParseExpression(cursor));
        while (cursor.TryConsume(Tokens::Comma)) {
            query_ast.group_by.push_back(ParseExpression(cursor));
        }
    }

    if (cursor.TryConsume(Tokens::Having)) {
        query_ast.having = ParsePredicate(cursor);
    }

    if (cursor.TryConsume(Tokens::Order)) {
        cursor.Consume(Tokens::By, "expected BY after ORDER");
        query_ast.order_by.push_back(ParseOrderByItem(cursor));
        while (cursor.TryConsume(Tokens::Comma)) {
            query_ast.order_by.push_back(ParseOrderByItem(cursor));
        }
    }

    query_ast.limit = ParseOptionalSize(cursor, Tokens::Limit, "LIMIT");
    if (const auto offset = ParseOptionalSize(cursor, Tokens::Offset, "OFFSET"); offset.has_value()) {
        query_ast.offset = *offset;
    }

    cursor.TryConsume(Tokens::Semicolon);
    cursor.Consume(Tokens::EndOfInput, "unexpected trailing tokens");

    return query_ast;
}
