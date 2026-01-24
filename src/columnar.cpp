#include "columnar.h"

#include "batch_io.h"
#include "error.h"
#include "schema.h"

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
