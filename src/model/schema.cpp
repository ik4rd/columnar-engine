#include "schema.h"

#include "csv.h"
#include "error.h"
#include "fileio.h"
#include "parsing.h"

Schema ReadSchemaCsv(const std::filesystem::path& path) {
    const CsvReader reader(path);

    Schema schema;
    std::vector<std::string> row;

    while (reader.ReadRow(row)) {
        if (row.size() == 1 && row[0].empty()) {
            continue;
        }
        if (row.size() != 2) {
            throw Error::InvalidData("columnar", "schema csv must have 2 columns", path.string());
        }
        schema.columns.push_back(ColumnSchema{
            row[0],
            ParseColumnType(row[1]),
        });
    }

    if (schema.columns.empty()) {
        throw Error::InvalidData("columnar", "schema csv is empty", path.string());
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
