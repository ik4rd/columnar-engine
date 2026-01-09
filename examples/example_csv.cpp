#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "csv.h"
#include "fileio.h"

int main() {
    try {
        const std::filesystem::path path = std::filesystem::temp_directory_path() / "csv_example.csv";

        const std::vector<std::vector<std::string>> rows = {
            {"id", "name", "notes"},         {"1", "alpha", "plain"},
            {"2", "be,ta", "comma, inside"}, {"3", "ga\"mma", "quote \" and ,"},
            {"4", "delta", "line1\nline2"},  {"5", "", ""},
        };

        {
            FileWriter writer(path);
            auto& out = writer.Stream();
            for (const auto& row : rows) {
                WriteCsvRow(out, row);
            }
            writer.Flush();
        }

        FileReader reader(path);
        auto& in = reader.Stream();

        std::vector<std::vector<std::string>> read_back;
        std::vector<std::string> row;
        while (ReadCsvRow(in, row)) {
            read_back.push_back(row);
        }

        std::cout << "rows written: " << rows.size() << std::endl;
        std::cout << "rows read: " << read_back.size() << std::endl;
        for (size_t i = 0; i < read_back.size(); ++i) {
            std::cout << "row " << i << ":";
            for (const auto& value : read_back[i]) {
                std::cout << " " << std::quoted(value);
            }
            std::cout << std::endl;
        }

        if (read_back != rows) {
            std::cerr << "csv roundtrip mismatch" << std::endl;
            return 2;
        }

        std::error_code ec;
        std::filesystem::remove(path, ec);
    } catch (const std::exception& ex) {
        std::cerr << "csv example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
