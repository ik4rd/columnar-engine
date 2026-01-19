#include "columnar.h"

#include "batch_io.h"
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

void ConvertCsvToColumnar(const std::filesystem::path& schema_path, const std::filesystem::path& data_path,
                          const std::filesystem::path& output_path, size_t max_rows_per_group) {
    if (max_rows_per_group == 0) {
        throw error::MakeError("columnar", "row group size must be > 0");
    }

    const Schema schema = ReadSchemaCsv(schema_path);
    BatchSizing sizing;
    sizing.max_rows = max_rows_per_group;
    CsvBatchReader batch_reader(data_path, schema, sizing);
    ColumnarBatchWriter batch_writer(output_path, schema);

    while (auto batch = batch_reader.ReadNext()) {
        batch_writer.Write(*batch);
    }

    batch_writer.Finalize();
}

void ConvertColumnarToCsv(const std::filesystem::path& columnar_path, const std::filesystem::path& schema_path,
                          const std::filesystem::path& data_path) {
    ColumnarBatchReader batch_reader(columnar_path);
    WriteSchemaCsv(schema_path, batch_reader.GetSchema());

    CsvBatchWriter batch_writer(data_path, batch_reader.GetSchema());
    while (auto batch = batch_reader.ReadNext()) {
        batch_writer.Write(*batch);
    }
    batch_writer.Flush();
}
