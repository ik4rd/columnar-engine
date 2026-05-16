#include "io/csv.h"

#include <optional>
#include <utility>

#include "io/file.h"
#include "common/error.h"

class CsvCharReader {
   public:
    explicit CsvCharReader(std::istream& in) : buffer_(in.rdbuf()) {}

   public:
    std::optional<char> Read() const {
        const Traits::int_type next = buffer_->sbumpc();
        if (Traits::eq_int_type(next, Traits::eof())) {
            return std::nullopt;
        }
        return Traits::to_char_type(next);
    }

    std::optional<char> Peek() const {
        const Traits::int_type next = buffer_->sgetc();
        if (Traits::eq_int_type(next, Traits::eof())) {
            return std::nullopt;
        }
        return Traits::to_char_type(next);
    }

    void Discard() const { static_cast<void>(Read()); }

   private:
    using Traits = std::streambuf::traits_type;

    std::streambuf* buffer_;
};

static bool NeedsCsvQuotes(const std::string_view value) {
    return value.find_first_of(",\"\n\r") != std::string_view::npos;
}

CsvReader::CsvReader(std::istream& in) : in_(&in) {}

CsvReader::CsvReader(const std::filesystem::path& path) : owned_in_(OpenInputFile(path)), in_(&owned_in_) {}

CsvReader::CsvReader(CsvReader&& other) noexcept : owned_in_(std::move(other.owned_in_)) {
    RebindAfterMove(other.in_ == &other.owned_in_, other.in_);
    other.in_ = nullptr;
}

CsvReader& CsvReader::operator=(CsvReader&& other) noexcept {
    if (this != &other) {
        owned_in_ = std::move(other.owned_in_);
        RebindAfterMove(other.in_ == &other.owned_in_, other.in_);
        other.in_ = nullptr;
    }
    return *this;
}

void CsvReader::RebindAfterMove(const bool uses_owned_stream, std::istream* source_stream) noexcept {
    if (uses_owned_stream) {
        in_ = &owned_in_;
        return;
    }
    in_ = source_stream;
}

bool CsvReader::ReadRow(std::vector<std::string>& row) const {
    row.clear();
    row.emplace_back();

    const CsvCharReader input(*in_);

    bool in_quotes = false;
    bool saw_data = false;

    while (true) {
        const std::optional<char> next = input.Read();
        if (!next.has_value()) {
            if (!saw_data) {
                return false;
            }
            if (in_quotes) {
                throw Error::MalformedData("csv", "unexpected EOF inside quoted field");
            }
            return true;
        }

        saw_data = true;
        const char ch = *next;

        if (in_quotes) {
            if (ch == '"') {
                if (input.Peek() == '"') {
                    input.Discard();
                    row.back().push_back('"');
                } else {
                    in_quotes = false;
                }
            } else {
                row.back().push_back(ch);
            }
            continue;
        }

        if (ch == '"') {
            if (row.back().empty()) {
                in_quotes = true;
            } else {
                row.back().push_back('"');
            }
            continue;
        }

        if (ch == ',') {
            row.emplace_back();
            continue;
        }

        if (ch == '\n') {
            return true;
        }

        if (ch == '\r') {
            if (input.Peek() == '\n') {
                input.Discard();
            }
            return true;
        }

        row.back().push_back(ch);
    }
}

CsvWriter::CsvWriter(std::ostream& out) : out_(&out) {}

CsvWriter::CsvWriter(const std::filesystem::path& path) : owned_out_(OpenOutputFile(path)), out_(&owned_out_) {}

CsvWriter::CsvWriter(CsvWriter&& other) noexcept : owned_out_(std::move(other.owned_out_)) {
    RebindAfterMove(other.out_ == &other.owned_out_, other.out_);
    other.out_ = nullptr;
}

CsvWriter& CsvWriter::operator=(CsvWriter&& other) noexcept {
    if (this != &other) {
        owned_out_ = std::move(other.owned_out_);
        RebindAfterMove(other.out_ == &other.owned_out_, other.out_);
        other.out_ = nullptr;
    }
    return *this;
}

void CsvWriter::RebindAfterMove(const bool uses_owned_stream, std::ostream* source_stream) noexcept {
    if (uses_owned_stream) {
        out_ = &owned_out_;
        return;
    }
    out_ = source_stream;
}

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
