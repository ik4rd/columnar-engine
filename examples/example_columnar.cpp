#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "columnar.h"
#include "csv.h"

void PrintRows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row : rows) {
        std::cout << "row:";
        for (const auto& value : row) {
            std::cout << " [" << value << "]";
        }
        std::cout << std::endl;
    }
}

bool SchemasEqual(const Schema& left, const Schema& right) {
    if (left.columns.size() != right.columns.size()) {
        return false;
    }

    for (size_t i = 0; i < left.columns.size(); ++i) {
        if (left.columns[i].name != right.columns[i].name) {
            return false;
        }
        if (left.columns[i].type != right.columns[i].type) {
            return false;
        }
    }

    return true;
}

int main() {
    try {
        const std::filesystem::path base_dir = std::filesystem::path(__FILE__).parent_path();
        const std::filesystem::path schema_in = base_dir / "schema.csv";
        const std::filesystem::path data_in = base_dir / "data.csv";
        const std::filesystem::path out_dir = base_dir / "out";
        std::filesystem::create_directories(out_dir);

        const std::filesystem::path columnar_file = out_dir / "output.columnar";
        const std::filesystem::path schema_out = out_dir / "schema_out.csv";
        const std::filesystem::path data_out = out_dir / "data_out.csv";

        std::cout << "input schema: " << schema_in << std::endl;
        std::cout << "input data: " << data_in << std::endl;
        std::cout << "output columnar: " << columnar_file << std::endl;
        std::cout << "output schema: " << schema_out << std::endl;
        std::cout << "output data: " << data_out << std::endl;

        constexpr size_t kRowGroupSize = 2;
        ConvertCsvToColumnar(schema_in, data_in, columnar_file, kRowGroupSize);
        ConvertColumnarToCsv(columnar_file, schema_out, data_out);

        const Schema schema_before = ReadSchemaCsv(schema_in);
        const Schema schema_after = ReadSchemaCsv(schema_out);
        if (!SchemasEqual(schema_before, schema_after)) {
            std::cerr << "schema roundtrip mismatch" << std::endl;
            return 1;
        }

        const auto data_rows = ReadRows(data_in);
        const auto roundtrip = ReadRows(data_out);
        std::cout << "roundtrip rows: " << roundtrip.size() << std::endl;
        PrintRows(roundtrip);

        if (roundtrip != data_rows) {
            std::cerr << "data roundtrip mismatch" << std::endl;
            return 1;
        }
    } catch (const std::exception& ex) {
        std::cerr << "columnar example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
