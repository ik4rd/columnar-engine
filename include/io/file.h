#pragma once

#include <concepts>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <vector>

struct FileMetadata {
    uintmax_t size = 0;
    bool is_regular = false;
    bool is_directory = false;
    std::filesystem::file_time_type last_write_time;
};

std::optional<FileMetadata> TryGetFileMetadata(const std::filesystem::path& path);

bool FileExists(const std::filesystem::path& path);
void EnsureParentDirectory(const std::filesystem::path& path);

std::ifstream OpenInputFile(const std::filesystem::path& path);
void SeekInputFile(std::istream& in, const std::filesystem::path& path, uint64_t offset);

class InputFile {
   public:
    explicit InputFile(std::filesystem::path path);
    InputFile(const InputFile&) = delete;
    InputFile(InputFile&&) noexcept = default;
    InputFile& operator=(const InputFile&) = delete;
    InputFile& operator=(InputFile&&) noexcept = default;
    ~InputFile() = default;

   public:
    void ReadAt(char* dst, size_t size, uint64_t offset);
    std::string ReadStringAt(uint64_t offset, size_t size);
    std::istream& StreamAt(uint64_t offset);

    template <std::integral T>
    T ReadAt(const uint64_t offset) {
        T value = 0;
        ReadAt(reinterpret_cast<char*>(&value), sizeof(value), offset);
        return value;
    }

   private:
    std::filesystem::path path_;
    std::ifstream in_;
};

enum class FileOpenMode {
    Truncate,
    Append,
};

std::ofstream OpenOutputFile(const std::filesystem::path& path, FileOpenMode mode = FileOpenMode::Truncate);

std::string ReadTextFile(const std::filesystem::path& path);
std::vector<uint8_t> ReadFileBytes(const std::filesystem::path& path);
void WriteFileBytes(const std::filesystem::path& path, std::span<const uint8_t> bytes);
void WriteFileBytes(const std::filesystem::path& path, const std::vector<uint8_t>& bytes);
