#include "schema.h"

#include "csv.h"
#include "error.h"
#include "fileio.h"
#include "parsing.h"

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
            throw error::MakeError("columnar", "schema csv must have 2 columns");
        }
        schema.columns.push_back(ColumnSchema{
            row[0],
            ParseColumnType(row[1]),
        });
    }

    if (schema.columns.empty()) {
        throw error::MakeError("columnar", "schema csv is empty");
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
