#include "fileio.h"

#include <fstream>

#include "error.h"

std::optional<FileMetadata> GetFileMetadata(const std::filesystem::path& path) {
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

std::vector<uint8_t> ReadFileBytes(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        throw error::MakePathError("fileio", path, "open for read");
    }

    const std::ifstream::pos_type end_pos = file.tellg();
    if (end_pos < 0) {
        throw error::MakePathError("fileio", path, "read file size");
    }

    std::vector<uint8_t> data(end_pos);
    if (end_pos == 0) {
        return data;
    }

    file.seekg(0, std::ios::beg);
    file.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (!file) {
        throw error::MakePathError("fileio", path, "read file");
    }

    return data;
}

void WriteFileBytes(const std::filesystem::path& path, const std::span<const uint8_t> bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        throw error::MakePathError("fileio", path, "open for write");
    }

    if (!bytes.empty()) {
        file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!file) {
            throw error::MakePathError("fileio", path, "write file");
        }
    }
}

void WriteFileBytes(const std::filesystem::path& path, const std::vector<uint8_t>& bytes) {
    WriteFileBytes(path, std::span(bytes.data(), bytes.size()));
}

void AppendFileBytes(const std::filesystem::path& path, const std::span<const uint8_t> bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::app);
    if (!file.is_open()) {
        throw error::MakePathError("fileio", path, "open for append");
    }

    if (!bytes.empty()) {
        file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!file) {
            throw error::MakePathError("fileio", path, "append file");
        }
    }
}

FileReader::FileReader(const std::filesystem::path& path) : path_(path), in_(path, std::ios::binary) {
    if (!in_.is_open()) {
        throw error::MakePathError("fileio", path_, "open for read");
    }
}

FileReader::~FileReader() {
    if (in_.is_open()) {
        in_.close();
    }
}

void FileReader::Seek(const std::streampos& pos) {
    in_.seekg(pos);
    if (!in_) {
        throw error::MakePathError("fileio", path_, "seek file");
    }
}

std::streampos FileReader::Tell() {
    const std::streampos pos = in_.tellg();
    if (pos == std::streampos(-1)) {
        throw error::MakePathError("fileio", path_, "tell file");
    }
    return pos;
}

void FileReader::Close() {
    if (in_.is_open()) {
        in_.close();
    }
}

FileWriter::FileWriter(const std::filesystem::path& path, FileOpenMode mode)
    : path_(path), out_(path, std::ios::binary | (mode == FileOpenMode::Append ? std::ios::app : std::ios::trunc)) {
    if (!out_.is_open()) {
        throw error::MakePathError("fileio", path_, "open for write");
    }
}

FileWriter::~FileWriter() {
    if (out_.is_open()) {
        out_.close();
    }
}

void FileWriter::Flush() {
    out_.flush();
    if (!out_) {
        throw error::MakePathError("fileio", path_, "write file");
    }
}

void FileWriter::Close() {
    if (out_.is_open()) {
        out_.close();
    }
}
