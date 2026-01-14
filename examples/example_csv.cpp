#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "csv.h"
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
        const TempFile csv_file("csv_example");

        const std::vector<std::vector<std::string>> rows = {
            {"id", "name", "notes"},         {"1", "alpha", "plain"},
            {"2", "be,ta", "comma, inside"}, {"3", "ga\"mma", "quote \" and ,"},
            {"4", "delta", "line1\nline2"},  {"5", "", ""},
        };

        WriteRows(csv_file.Path(), rows);

        const auto read_back = ReadRows(csv_file.Path());
        std::cout << "roundtrip rows: " << read_back.size() << std::endl;
        PrintRows(read_back);

        if (read_back != rows) {
            std::cerr << "csv roundtrip mismatch" << std::endl;
            return 1;
        }
    } catch (const std::exception& ex) {
        std::cerr << "csv example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
