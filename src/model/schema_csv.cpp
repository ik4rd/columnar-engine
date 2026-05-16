#include "model/schema_csv.h"

#include <array>
#include <string>
#include <vector>

#include "io/csv.h"
#include "common/error.h"
#include "common/parsing.h"

static bool CanParseAs(const ColumnType type, const std::string& value) {
    switch (type) {
        case ColumnType::Boolean:
            return TryParseBoolean(value).has_value();
        case ColumnType::Int16:
            return TryParseInt16(value).has_value();
        case ColumnType::Int32:
            return TryParseInt32(value).has_value();
        case ColumnType::Int64:
            return TryParseInt64(value).has_value();
        case ColumnType::Int128:
            return TryParseInt128(value).has_value();
        case ColumnType::Date:
            return TryParseDate(value).has_value();
        case ColumnType::Timestamp:
            return TryParseTimestamp(value).has_value();
        case ColumnType::Character:
            return TryParseCharacter(value).has_value();
        case ColumnType::String:
            return true;
    }

    throw Error::Unsupported("schema", "unknown column type");
}

static ColumnType InferColumnType(const std::vector<std::string>& values) {
    static constexpr std::array InferenceOrder = {
        ColumnType::Boolean, ColumnType::Int16,     ColumnType::Int32,     ColumnType::Int64,  ColumnType::Int128,
        ColumnType::Date,    ColumnType::Timestamp, ColumnType::Character, ColumnType::String,
    };

    for (const ColumnType type : InferenceOrder) {
        bool matches = true;
        for (const auto& value : values) {
            if (!CanParseAs(type, value)) {
                matches = false;
                break;
            }
        }
        if (matches) {
            return type;
        }
    }

    return ColumnType::String;
}

Schema ReadSchemaCsv(const std::filesystem::path& path) {
    const CsvReader reader(path);

    Schema schema;
    std::vector<std::string> row;

    while (reader.ReadRow(row)) {
        if (row.size() == 1 && row[0].empty()) {
            continue;
        }
        while (row.size() > 2 && row.back().empty()) {
            row.pop_back();
        }
        if (row.size() != 2) {
            throw Error::MalformedData("schema", "schema csv must have 2 columns", path.string());
        }
        schema.columns.push_back(ColumnSchema{
            row[0],
            ParseColumnType(row[1]),
        });
    }

    if (schema.columns.empty()) {
        throw Error::MalformedData("schema", "schema csv is empty", path.string());
    }

    return schema;
}

Schema InferSchemaCsv(const std::filesystem::path& path) {
    const CsvReader reader(path);

    std::vector<std::vector<std::string>> values_by_column;
    std::vector<std::string> row;

    while (reader.ReadRow(row)) {
        if (values_by_column.empty()) {
            values_by_column.resize(row.size());
        } else if (row.size() != values_by_column.size()) {
            throw Error::MalformedData("schema", "csv rows have inconsistent column count", path.string());
        }

        for (size_t i = 0; i < row.size(); ++i) {
            values_by_column[i].push_back(row[i]);
        }
    }

    if (values_by_column.empty()) {
        throw Error::MalformedData("schema", "csv is empty", path.string());
    }

    Schema schema;
    schema.columns.reserve(values_by_column.size());

    for (size_t i = 0; i < values_by_column.size(); ++i) {
        schema.columns.push_back(ColumnSchema{
            "column_" + std::to_string(i + 1),
            InferColumnType(values_by_column[i]),
        });
    }

    return schema;
}

void WriteSchemaCsv(const std::filesystem::path& path, const Schema& schema) {
    const CsvWriter writer(path);

    for (const auto& [name, type] : schema.columns) {
        writer.WriteRow({name, ColumnTypeToString(type)});
    }

    writer.Flush();
}
