#include "model/schema_csv.h"

#include <array>
#include <string>
#include <vector>

#include "io/csv.h"
#include "support/error.h"
#include "support/parsing.h"

static bool CanParseAs(const ColumnType type, const std::string& value) {
    try {
        switch (type) {
            case ColumnType::Boolean:
                static_cast<void>(ParseBoolean(value));
                return true;
            case ColumnType::Int16:
                static_cast<void>(ParseInt16(value));
                return true;
            case ColumnType::Int32:
                static_cast<void>(ParseInt32(value));
                return true;
            case ColumnType::Int64:
                static_cast<void>(ParseInt64(value));
                return true;
            case ColumnType::Int128:
                static_cast<void>(ParseInt128(value));
                return true;
            case ColumnType::Date:
                static_cast<void>(ParseDate(value));
                return true;
            case ColumnType::Timestamp:
                static_cast<void>(ParseTimestamp(value));
                return true;
            case ColumnType::Character:
                static_cast<void>(ParseCharacter(value));
                return true;
            case ColumnType::String:
                return true;
        }
    } catch (const Error&) {
        return false;
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
            throw Error::InvalidData("schema", "schema csv must have 2 columns", path.string());
        }
        schema.columns.push_back(ColumnSchema{
            row[0],
            ParseColumnType(row[1]),
        });
    }

    if (schema.columns.empty()) {
        throw Error::InvalidData("schema", "schema csv is empty", path.string());
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
            throw Error::InvalidData("schema", "csv rows have inconsistent column count", path.string());
        }

        for (size_t i = 0; i < row.size(); ++i) {
            values_by_column[i].push_back(row[i]);
        }
    }

    if (values_by_column.empty()) {
        throw Error::InvalidData("schema", "csv is empty", path.string());
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
