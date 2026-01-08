#include "fileio.h"

#include <fstream>
#include <stdexcept>
#include <string>

namespace {
std::runtime_error MakeError(const std::filesystem::path& path, const std::string& action) {
    return std::runtime_error("fileio: failed to " + action + " '" + path.string() + "'");
}
}  // namespace

std::optional<FileMetadata> GetFileMetadata(const std::filesystem::path& path) {
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        if (ec) {
            throw MakeError(path, "check existence");
        }
        return std::nullopt;
    }

    FileMetadata metadata;
    const auto status = std::filesystem::status(path, ec);
    if (ec) {
        throw MakeError(path, "read status");
    }

    metadata.is_regular = std::filesystem::is_regular_file(status);
    metadata.is_directory = std::filesystem::is_directory(status);
    if (metadata.is_regular) {
        metadata.size = std::filesystem::file_size(path, ec);
        if (ec) {
            throw MakeError(path, "read file size");
        }
    }

    metadata.last_write_time = std::filesystem::last_write_time(path, ec);
    if (ec) {
        throw MakeError(path, "read last write time");
    }

    return metadata;
}

bool FileExists(const std::filesystem::path& path) {
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) {
        throw MakeError(path, "check existence");
    }
    return exists;
}

std::vector<std::uint8_t> ReadFileBytes(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        throw MakeError(path, "open for read");
    }

    const std::ifstream::pos_type end_pos = file.tellg();
    if (end_pos < 0) {
        throw MakeError(path, "read file size");
    }

    std::vector<std::uint8_t> data(end_pos);
    if (end_pos == 0) {
        return data;
    }

    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (!file) {
        throw MakeError(path, "read file");
    }

    return data;
}

void WriteFileBytes(const std::filesystem::path& path, const std::span<const std::uint8_t> bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        throw MakeError(path, "open for write");
    }

    if (!bytes.empty()) {
        file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!file) {
            throw MakeError(path, "write file");
        }
    }
}

void WriteFileBytes(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    WriteFileBytes(path, std::span(bytes.data(), bytes.size()));
}

void AppendFileBytes(const std::filesystem::path& path, const std::span<const std::uint8_t> bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::app);
    if (!file.is_open()) {
        throw MakeError(path, "open for append");
    }

    if (!bytes.empty()) {
        file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!file) {
            throw MakeError(path, "append file");
        }
    }
}
