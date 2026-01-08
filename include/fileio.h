#pragma once

#include <filesystem>
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
