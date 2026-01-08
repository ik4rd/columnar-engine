#pragma once

#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <vector>

struct FileMetadata {
    uintmax_t size = 0;
    bool is_regular = false;
    bool is_directory = false;
    std::filesystem::file_time_type last_write_time;
};

std::optional<FileMetadata> GetFileMetadata(const std::filesystem::path& path);

bool FileExists(const std::filesystem::path& path);

std::vector<uint8_t> ReadFileBytes(const std::filesystem::path& path);

void WriteFileBytes(const std::filesystem::path& path, std::span<const uint8_t> bytes);

void WriteFileBytes(const std::filesystem::path& path, const std::vector<uint8_t>& bytes);

void AppendFileBytes(const std::filesystem::path& path, std::span<const uint8_t> bytes);

enum class FileOpenMode {
    Truncate,
    Append,
};

class FileReader {
   public:
    explicit FileReader(const std::filesystem::path& path);
    ~FileReader();

    const std::filesystem::path& Path() const { return path_; }
    std::istream& Stream() { return in_; }

    void Seek(const std::streampos& pos);
    std::streampos Tell();
    void Close();

   private:
    std::filesystem::path path_;
    std::ifstream in_;
};

class FileWriter {
   public:
    explicit FileWriter(const std::filesystem::path& path, FileOpenMode mode = FileOpenMode::Truncate);
    ~FileWriter();

    const std::filesystem::path& Path() const { return path_; }
    std::ostream& Stream() { return out_; }

    void Flush();
    void Close();

   private:
    std::filesystem::path path_;
    std::ofstream out_;
};
