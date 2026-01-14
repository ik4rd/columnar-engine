#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "columnar.h"
#include "csv.h"
#include "fileio.h"
#include "temp_file.h"

void PrintRows(const std::vector<std::vector<std::string>>& rows) {
    for (const auto& row : rows) {
        std::cout << "row:";
        for (const auto& value : row) {
            std::cout << " [" << value << "]";
        }
        std::cout << std::endl;
    }
}

int main() {
    try {
        const TempFile schema_in("schema_in");
        const TempFile data_in("data_in");
        const TempFile columnar_file("data_columnar");
        const TempFile schema_out("schema_out");
        const TempFile data_out("data_out");

        const std::vector<std::vector<std::string>> schema_rows = {
            {"id", "int64"},
            {"name", "string"},
            {"score", "int64"},
        };
        const std::vector<std::vector<std::string>> data_rows = {
            {"1", "alpha", "10"},
            {"2", "be,ta", "20"},
            {"3", "ga\"mma", "30"},
        };

        WriteRows(schema_in.Path(), schema_rows);
        WriteRows(data_in.Path(), data_rows);

        constexpr size_t kRowGroupSize = 2;
        ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), kRowGroupSize);
        ConvertColumnarToCsv(columnar_file.Path(), schema_out.Path(), data_out.Path());

        const auto roundtrip = ReadRows(data_out.Path());
        std::cout << "roundtrip rows: " << roundtrip.size() << std::endl;
        PrintRows(roundtrip);

        if (roundtrip != data_rows) {
            std::cerr << "roundtrip mismatch" << std::endl;
            return 1;
        }
    } catch (const std::exception& ex) {
        std::cerr << "columnar example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
