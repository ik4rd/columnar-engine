#include "io/csv.h"

#include <utility>

#include "common/error.h"
#include "io/file.h"

constexpr char CsvDelimiter = ',';
constexpr char CsvQuote = '"';
constexpr char CsvLf = '\n';
constexpr char CsvCr = '\r';
constexpr std::string_view CsvQuotedChars = ",\"\n\r";

class CsvCharReader {
   public:
    explicit CsvCharReader(const std::istream& in) : buffer_(in.rdbuf()) {}

   private:
    using Traits = std::streambuf::traits_type;

   public:
    Traits::int_type Read() const { return buffer_->sbumpc(); }

    Traits::int_type Peek() const { return buffer_->sgetc(); }

    static bool IsEof(const Traits::int_type value) { return Traits::eq_int_type(value, Traits::eof()); }

    static char ToChar(const Traits::int_type value) { return Traits::to_char_type(value); }

    void Discard() const {
        if (!IsEof(Peek())) {
            buffer_->sbumpc();
        }
    }

    std::streambuf* buffer_;
};

static bool NeedsCsvQuotes(const std::string_view value) {
    return value.find_first_of(CsvQuotedChars) != std::string_view::npos;
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
        const auto next = input.Read();

        if (input.IsEof(next)) {
            if (!saw_data) {
                return false;
            }
            if (in_quotes) {
                throw Error::MalformedData("io", "unexpected EOF inside quoted field");
            }
            return true;
        }

        saw_data = true;
        const char ch = input.ToChar(next);

        if (in_quotes) {
            if (ch == CsvQuote) {
                const auto peek = input.Peek();
                if (!input.IsEof(peek) && input.ToChar(peek) == CsvQuote) {
                    input.Discard();
                    row.back().push_back(CsvQuote);
                } else {
                    in_quotes = false;
                }
            } else {
                row.back().push_back(ch);
            }
            continue;
        }

        if (ch == CsvQuote) {
            if (row.back().empty()) {
                in_quotes = true;
            } else {
                row.back().push_back(CsvQuote);
            }
            continue;
        }

        if (ch == CsvDelimiter) {
            row.emplace_back();
            continue;
        }

        if (ch == CsvLf) {
            return true;
        }

        if (ch == CsvCr) {
            const auto peek = input.Peek();
            if (!input.IsEof(peek) && input.ToChar(peek) == CsvLf) {
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
            *out_ << CsvDelimiter;
        }

        const std::string& value = row[i];
        if (!NeedsCsvQuotes(value)) {
            *out_ << value;
            continue;
        }

        *out_ << CsvQuote;
        for (const char ch : value) {
            if (ch == CsvQuote) {
                *out_ << "\"\"";
            } else {
                *out_ << ch;
            }
        }

        *out_ << CsvQuote;
    }

    *out_ << CsvLf;

    if (!*out_) {
        throw Error::Io("io", "failed to write csv row");
    }
}

void CsvWriter::Flush() const {
    out_->flush();
    if (!*out_) {
        throw Error::Io("io", "failed to write csv");
    }
}

std::vector<std::vector<std::string>> ReadRows(const std::filesystem::path& path) {
    const CsvReader reader(path);

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> row;

    while (reader.ReadRow(row)) {
        rows.push_back(std::move(row));
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
