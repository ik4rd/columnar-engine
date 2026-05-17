#pragma once

#include <filesystem>
#include <fstream>
#include <iosfwd>
#include <string>
#include <vector>

class CsvReader {
   public:
    explicit CsvReader(std::istream& in);
    explicit CsvReader(const std::filesystem::path& path);
    CsvReader(const CsvReader&) = delete;
    CsvReader(CsvReader&& other) noexcept;
    CsvReader& operator=(const CsvReader&) = delete;
    CsvReader& operator=(CsvReader&& other) noexcept;
    ~CsvReader() = default;

    bool ReadRow(std::vector<std::string>& row) const;

   private:
    void RebindAfterMove(bool uses_owned_stream, std::istream* source_stream) noexcept;

    std::ifstream owned_in_;
    std::istream* in_ = nullptr;
};

class CsvWriter {
   public:
    explicit CsvWriter(std::ostream& out);
    explicit CsvWriter(const std::filesystem::path& path);
    CsvWriter(const CsvWriter&) = delete;
    CsvWriter(CsvWriter&& other) noexcept;
    CsvWriter& operator=(const CsvWriter&) = delete;
    CsvWriter& operator=(CsvWriter&& other) noexcept;
    ~CsvWriter() = default;

    void WriteRow(const std::vector<std::string>& row) const;
    void Flush() const;

   private:
    void RebindAfterMove(bool uses_owned_stream, std::ostream* source_stream) noexcept;

    std::ofstream owned_out_;
    std::ostream* out_ = nullptr;
};

std::vector<std::vector<std::string>> ReadRows(const std::filesystem::path& path);
void WriteRows(const std::filesystem::path& path, const std::vector<std::vector<std::string>>& rows);
