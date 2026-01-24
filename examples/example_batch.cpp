#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "batch_io.h"
#include "csv.h"
#include "schema.h"

void PrintRows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row : rows) {
        std::cout << "row:";
        for (const auto& value : row) {
            std::cout << " [" << value << "]";
        }
        std::cout << std::endl;
    }
}

void AppendBatchRows(const Batch& batch, std::vector<std::vector<std::string>>& rows) {
    const size_t row_count = batch.RowsCount();
    const size_t column_count = batch.ColumnsCount();

    for (size_t row = 0; row < row_count; ++row) {
        std::vector<std::string> values;
        values.reserve(column_count);
        for (size_t col = 0; col < column_count; ++col) {
            values.push_back(batch.ColumnAt(col).ValueAsString(row));
        }
        rows.push_back(std::move(values));
    }
}

int main() {
    try {
        const std::filesystem::path base_dir = std::filesystem::path(__FILE__).parent_path();
        const std::filesystem::path schema_in = base_dir / "schema.csv";
        const std::filesystem::path data_in = base_dir / "data.csv";
        const std::filesystem::path out_dir = base_dir / "out_batch";
        std::filesystem::create_directories(out_dir);

        const std::filesystem::path columnar_file = out_dir / "output_batch.columnar";

        std::cout << "input schema: " << schema_in << std::endl;
        std::cout << "input data: " << data_in << std::endl;
        std::cout << "output columnar: " << columnar_file << std::endl;

        const Schema schema = ReadSchemaCsv(schema_in);
        BatchSizing sizing;
        sizing.max_rows = 2;

        CsvBatchReader csv_reader(data_in, schema, sizing);
        ColumnarBatchWriter columnar_writer(columnar_file, schema);

        size_t batch_index = 0;
        while (auto batch = csv_reader.ReadNext()) {
            std::cout << "writing batch " << batch_index++ << " rows: " << batch->RowsCount() << std::endl;
            columnar_writer.Write(*batch);
        }
        columnar_writer.Finalize();

        ColumnarBatchReader columnar_reader(columnar_file);
        std::vector<std::vector<std::string>> roundtrip_rows;
        while (auto batch = columnar_reader.ReadNext()) {
            AppendBatchRows(*batch, roundtrip_rows);
        }

        const auto original_rows = ReadRows(data_in);
        std::cout << "roundtrip rows: " << roundtrip_rows.size() << std::endl;
        PrintRows(roundtrip_rows);

        if (roundtrip_rows != original_rows) {
            std::cerr << "batch roundtrip mismatch" << std::endl;
            return 1;
        }
    } catch (const std::exception& ex) {
        std::cerr << "batch example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
