#include "fileio.h"

#include "error.h"

std::optional<FileMetadata> TryGetFileMetadata(const std::filesystem::path& path) {
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        if (ec) {
            throw error::MakePathError("fileio", path, "check existence");
        }
        return std::nullopt;
    }

    FileMetadata metadata;
    const auto status = std::filesystem::status(path, ec);
    if (ec) {
        throw error::MakePathError("fileio", path, "read status");
    }

    metadata.is_regular = std::filesystem::is_regular_file(status);
    metadata.is_directory = std::filesystem::is_directory(status);
    if (metadata.is_regular) {
        metadata.size = std::filesystem::file_size(path, ec);
        if (ec) {
            throw error::MakePathError("fileio", path, "read file size");
        }
    }

    metadata.last_write_time = std::filesystem::last_write_time(path, ec);
    if (ec) {
        throw error::MakePathError("fileio", path, "read last write time");
    }

    return metadata;
}

bool FileExists(const std::filesystem::path& path) {
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) {
        throw error::MakePathError("fileio", path, "check existence");
    }
    return exists;
}

std::ifstream OpenInputFile(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        throw error::MakePathError("fileio", path, "open for read");
    }
    return file;
}

std::ofstream OpenOutputFile(const std::filesystem::path& path, const FileOpenMode mode) {
    std::ofstream file(path, std::ios::binary | (mode == FileOpenMode::Append ? std::ios::app : std::ios::trunc));
    if (!file.is_open()) {
        throw error::MakePathError("fileio", path, mode == FileOpenMode::Append ? "open for append" : "open for write");
    }
    return file;
}

std::vector<uint8_t> ReadFileBytes(const std::filesystem::path& path) {
    std::ifstream file = OpenInputFile(path);
    file.seekg(0, std::ios::end);
    if (!file) {
        throw error::MakePathError("fileio", path, "read file size");
    }

    const auto end_pos = file.tellg();
    if (end_pos < 0) {
        throw error::MakePathError("fileio", path, "read file size");
    }

    std::vector<uint8_t> data(end_pos);
    if (end_pos == 0) {
        return data;
    }

    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(data.data()), data.size());
    if (!file) {
        throw error::MakePathError("fileio", path, "read file");
    }

    return data;
}

void WriteFileBytes(const std::filesystem::path& path, const std::span<const uint8_t> bytes) {
    std::ofstream file = OpenOutputFile(path);
    if (!bytes.empty()) {
        file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        if (!file) {
            throw error::MakePathError("fileio", path, "write file");
        }
    }

    file.flush();
    if (!file) {
        throw error::MakePathError("fileio", path, "write file");
    }
}

void WriteFileBytes(const std::filesystem::path& path, const std::vector<uint8_t>& bytes) {
    WriteFileBytes(path, std::span(bytes.data(), bytes.size()));
}
