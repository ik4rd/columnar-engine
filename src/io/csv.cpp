#include "csv.h"

#include "error.h"
#include "fileio.h"

static bool NeedsCsvQuotes(const std::string_view value) {
    return value.find_first_of(",\"\n\r") != std::string_view::npos;
}

CsvReader::CsvReader(std::istream& in) : in_(&in) {}

CsvReader::CsvReader(const std::filesystem::path& path) : owned_in_(OpenInputFile(path)), in_(&owned_in_) {}

bool CsvReader::ReadRow(std::vector<std::string>& row) const {
    row.clear();

    std::string field;
    bool in_quotes = false;
    bool saw_data = false;

    while (true) {
        const int next = in_->get();
        if (next == EOF) {
            if (!saw_data) {
                return false;
            }
            if (in_quotes) {
                throw Error::InvalidData("csv", "unexpected EOF inside quoted field");
            }
            row.push_back(field);
            return true;
        }

        saw_data = true;
        const char ch = next;

        if (in_quotes) {
            if (ch == '"') {
                const int peek = in_->peek();
                if (peek == '"') {
                    in_->get();
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
            if (in_->peek() == '\n') {
                in_->get();
            }
            row.push_back(field);
            return true;
        }

        field.push_back(ch);
    }
}

CsvWriter::CsvWriter(std::ostream& out) : out_(&out) {}

CsvWriter::CsvWriter(const std::filesystem::path& path) : owned_out_(OpenOutputFile(path)), out_(&owned_out_) {}

void CsvWriter::WriteRow(const std::vector<std::string>& row) const {
    for (size_t i = 0; i < row.size(); ++i) {
        if (i > 0) {
            *out_ << ',';
        }

        const std::string& value = row[i];
        if (!NeedsCsvQuotes(value)) {
            *out_ << value;
            continue;
        }

        *out_ << '"';
        for (const char ch : value) {
            if (ch == '"') {
                *out_ << "\"\"";
            } else {
                *out_ << ch;
            }
        }
        *out_ << '"';
    }
    *out_ << '\n';
    if (!*out_) {
        throw Error::Io("csv", "failed to write csv row");
    }
}

void CsvWriter::Flush() const {
    out_->flush();
    if (!*out_) {
        throw Error::Io("csv", "failed to write csv");
    }
}

std::vector<std::vector<std::string>> ReadRows(const std::filesystem::path& path) {
    const CsvReader reader(path);

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> row;

    while (reader.ReadRow(row)) {
        rows.push_back(row);
    }

    return rows;
}

void WriteRows(const std::filesystem::path& path, const std::vector<std::vector<std::string>>& rows) {
    const CsvWriter writer(path);

    for (const auto& row : rows) {
        writer.WriteRow(row);
    }

    writer.Flush();
}
