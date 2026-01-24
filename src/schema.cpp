#include "schema.h"

#include "csv.h"
#include "error.h"
#include "fileio.h"
#include "parsing.h"

/**
 * @brief Reads a schema definition from a CSV file and constructs a Schema.
 *
 * The CSV must contain one column definition per row with exactly two columns:
 * the column name and the column type (as parsed by ParseColumnType).
 * Rows consisting of a single empty string are ignored.
 *
 * @param path Filesystem path to the CSV file containing the schema.
 * @return Schema The parsed Schema with populated columns.
 * @throws error::Error if any CSV row does not have exactly two columns (message: "schema csv must have 2 columns").
 * @throws error::Error if the CSV contains no column definitions (message: "schema csv is empty").
 */
Schema ReadSchemaCsv(const std::filesystem::path& path) {
    FileReader reader(path);
    auto& in = reader.Stream();

    Schema schema;
    std::vector<std::string> row;

    while (ReadCsvRow(in, row)) {
        if (row.size() == 1 && row[0].empty()) {
            continue;
        }
        if (row.size() != 2) {
            throw error::MakeError("schema", "schema csv must have 2 columns");
        }
        schema.columns.push_back(ColumnSchema{
            row[0],
            ParseColumnType(row[1]),
        });
    }

    if (schema.columns.empty()) {
        throw error::MakeError("schema", "schema csv is empty");
    }

    return schema;
}

void WriteSchemaCsv(const std::filesystem::path& path, const Schema& schema) {
    FileWriter writer(path);
    auto& out = writer.Stream();

    for (const auto& [name, type] : schema.columns) {
        WriteCsvRow(out, {name, ColumnTypeToString(type)});
    }

    writer.Flush();
}