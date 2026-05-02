#include <algorithm>

#include "ascii.h"
#include "columnar_batch_io.h"
#include "executor.h"

static size_t FindColumnIndex(const Schema& schema, const std::string_view column_name) {
    const std::string needle = ToLowerAscii(column_name);
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (ToLowerAscii(schema.columns[i].name) == needle) {
            return i;
        }
    }
    throw Error::InvalidArgument("query_planner", "unknown column '" + std::string(column_name) + "'");
}

static std::string UnescapeSqlString(const std::string_view text) {
    if (text.size() < 2 || text.front() != '\'' || text.back() != '\'') {
        throw Error::InvalidArgument("query_planner", "invalid string literal");
    }

    std::string result;
    result.reserve(text.size() - 2);

    for (size_t i = 1; i + 1 < text.size(); ++i) {
        if (text[i] == '\'' && i + 2 < text.size() && text[i + 1] == '\'') {
            result.push_back('\'');
            ++i;
            continue;
        }
        result.push_back(text[i]);
    }

    return result;
}

static std::string NormalizeLiteral(const ParsedLiteral& literal, const ColumnType type) {
    switch (literal.token_type) {
        case Tokens::StringLiteral:
            return UnescapeSqlString(literal.text);
        case Tokens::NumericLiteral:
        case Tokens::NameToken:
            if (type == ColumnType::String && literal.token_type == Tokens::NameToken) {
                return literal.text;
            }
            return literal.text;
        default:
            throw Error::InvalidArgument("query_planner", "unsupported literal type");
    }
}

PlannedQuery PlanQuery(const ParsedQuery& parsed,
                       const std::unordered_map<std::string, std::filesystem::path>& tables) {
    PlannedQuery planned;

    const auto table_it = tables.find(ToLowerAscii(parsed.table_name));
    if (table_it == tables.end()) {
        throw Error::NotFound("query_planner", "unknown table", parsed.table_name);
    }

    planned.table_path = table_it->second;

    ColumnarBatchReader reader(planned.table_path);
    const Schema& schema = reader.GetSchema();

    const auto register_projection = [&](const size_t source_index) -> size_t {
        const auto it = std::ranges::find(planned.projection_indexes, source_index);
        if (it != planned.projection_indexes.end()) {
            return static_cast<size_t>(std::distance(planned.projection_indexes.begin(), it));
        }
        planned.projection_indexes.push_back(source_index);
        return planned.projection_indexes.size() - 1;
    };

    if (parsed.filter.has_value()) {
        const size_t source_index = FindColumnIndex(schema, parsed.filter->column_name);
        const ColumnType column_type = schema.columns[source_index].type;

        planned.filter = PlannedFilter{
            .column_index = register_projection(source_index),
            .column_type = column_type,
            .comparison = parsed.filter->comparison,
            .literal_text = NormalizeLiteral(parsed.filter->literal, column_type),
        };
    }

    planned.aggregates.reserve(parsed.aggregates.size());

    for (const auto& aggregate : parsed.aggregates) {
        PlannedAgg planned_aggregate{
            .function = aggregate.function,
            .distinct = aggregate.distinct,
            .star = aggregate.star,
            .column_index = std::nullopt,
            .input_type = ColumnType::String,
            .output_name = aggregate.output_name,
        };

        if (!aggregate.star) {
            const size_t source_index = FindColumnIndex(schema, aggregate.column_name);
            planned_aggregate.column_index = register_projection(source_index);
            planned_aggregate.input_type = schema.columns[source_index].type;
            if (!AggSupportsInputType(*aggregate.function, planned_aggregate.input_type)) {
                throw Error::Unsupported("query_planner",
                                         "aggregate " + std::string(aggregate.function->canonical_name) +
                                             " does not support column '" + aggregate.column_name + "'");
            }
        }

        planned.aggregates.push_back(std::move(planned_aggregate));
    }

    if (planned.projection_indexes.empty() && !schema.columns.empty()) {
        planned.projection_indexes.push_back(0);
    }

    return planned;
}
