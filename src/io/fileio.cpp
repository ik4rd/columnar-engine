#include "fileio.h"

#include <iterator>

#include "error.h"

std::optional<FileMetadata> TryGetFileMetadata(const std::filesystem::path& path) {
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        if (ec) {
            throw Error::PathIo("fileio", path, "check existence");
        }
        return std::nullopt;
    }

    FileMetadata metadata;
    const auto status = std::filesystem::status(path, ec);
    if (ec) {
        throw Error::PathIo("fileio", path, "read status");
    }

    metadata.is_regular = std::filesystem::is_regular_file(status);
    metadata.is_directory = std::filesystem::is_directory(status);
    if (metadata.is_regular) {
        metadata.size = std::filesystem::file_size(path, ec);
        if (ec) {
            throw Error::PathIo("fileio", path, "read file size");
        }
    }

    metadata.last_write_time = std::filesystem::last_write_time(path, ec);
    if (ec) {
        throw Error::PathIo("fileio", path, "read last write time");
    }

    return metadata;
}

bool FileExists(const std::filesystem::path& path) {
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) {
        throw Error::PathIo("fileio", path, "check existence");
    }
    return exists;
}

void EnsureParentDirectory(const std::filesystem::path& path) {
    const auto parent = path.parent_path();
    if (parent.empty()) {
        return;
    }

    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
        throw Error::PathIo("fileio", path, "create parent directories");
    }
}

std::ifstream OpenInputFile(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        throw Error::PathIo("fileio", path, "open for read");
    }
    return file;
}

std::ofstream OpenOutputFile(const std::filesystem::path& path, const FileOpenMode mode) {
    std::ofstream file(path, std::ios::binary | (mode == FileOpenMode::Append ? std::ios::app : std::ios::trunc));
    if (!file.is_open()) {
        throw Error::PathIo("fileio", path, mode == FileOpenMode::Append ? "open for append" : "open for write");
    }
    return file;
}

std::string ReadTextFile(const std::filesystem::path& path) {
    std::ifstream file = OpenInputFile(path);
    return std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
}

std::vector<uint8_t> ReadFileBytes(const std::filesystem::path& path) {
    std::ifstream file = OpenInputFile(path);
    file.seekg(0, std::ios::end);
    if (!file) {
        throw Error::PathIo("fileio", path, "read file size");
    }

    const auto end_pos = file.tellg();
    if (end_pos < 0) {
        throw Error::PathIo("fileio", path, "read file size");
    }

    std::vector<uint8_t> data(end_pos);
    if (end_pos == 0) {
        return data;
    }

    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(data.data()), data.size());

    if (!file) {
        throw Error::PathIo("fileio", path, "read file");
    }

    return data;
}

void WriteFileBytes(const std::filesystem::path& path, const std::span<const uint8_t> bytes) {
    std::ofstream file = OpenOutputFile(path);
    if (!bytes.empty()) {
        file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        if (!file) {
            throw Error::PathIo("fileio", path, "write file");
        }
    }

    file.flush();
    if (!file) {
        throw Error::PathIo("fileio", path, "write file");
    }
}

void WriteFileBytes(const std::filesystem::path& path, const std::vector<uint8_t>& bytes) {
    WriteFileBytes(path, std::span(bytes.data(), bytes.size()));
}
