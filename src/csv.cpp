#include "csv.h"

#include "fileio.h"
#include "utils.h"

bool ReadCsvRow(std::istream& in, std::vector<std::string>& row) {
    row.clear();

    std::string field;
    bool in_quotes = false;
    bool saw_data = false;

    while (true) {
        const int next = in.get();
        if (next == EOF) {
            if (!saw_data) {
                return false;
            }
            if (in_quotes) {
                throw std::runtime_error("csv: unexpected EOF inside quoted field");
            }
            row.push_back(field);
            return true;
        }

        saw_data = true;
        const char ch = next;

        if (in_quotes) {
            if (ch == '"') {
                const int peek = in.peek();
                if (peek == '"') {
                    in.get();
                    field.push_back('"');
                } else {
                    in_quotes = false;
                }
            } else {
                field.push_back(ch);
            }
            continue;
        }

        if (ch == '"') {
            if (field.empty()) {
                in_quotes = true;
            } else {
                field.push_back('"');
            }
            continue;
        }

        if (ch == ',') {
            row.push_back(field);
            field.clear();
            continue;
        }

        if (ch == '\n') {
            row.push_back(field);
            return true;
        }

        if (ch == '\r') {
            if (in.peek() == '\n') {
                in.get();
            }
            row.push_back(field);
            return true;
        }

        field.push_back(ch);
    }
}

void WriteCsvRow(std::ostream& out, const std::vector<std::string>& row) {
    for (size_t i = 0; i < row.size(); ++i) {
        if (i > 0) {
            out << ',';
        }

        const std::string& value = row[i];
        if (!NeedsQuotes(value)) {
            out << value;
            continue;
        }

        out << '"';
        for (const char ch : value) {
            if (ch == '"') {
                out << "\"\"";
            } else {
                out << ch;
            }
        }
        out << '"';
    }
    out << '\n';
}

std::vector<std::vector<std::string>> ReadRows(const std::filesystem::path& path) {
    FileReader reader(path);
    auto& in = reader.Stream();

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> row;

    while (ReadCsvRow(in, row)) {
        rows.push_back(row);
    }

    return rows;
}

void WriteRows(const std::filesystem::path& path, const std::vector<std::vector<std::string>>& rows) {
    FileWriter writer(path);
    auto& out = writer.Stream();

    for (const auto& row : rows) {
        WriteCsvRow(out, row);
    }

    writer.Flush();
}
