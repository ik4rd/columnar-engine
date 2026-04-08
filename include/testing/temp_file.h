/* (What is this? — Модуль временного файла) */

#pragma once

#include <chrono>
#include <filesystem>
#include <random>
#include <string>
#include <system_error>
#include <utility>

inline std::filesystem::path UniqueTempPath(const std::string& tag) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<unsigned long long> dist;

    return std::filesystem::temp_directory_path() / (tag + "_" + std::to_string(now) + "_" + std::to_string(dist(gen)));
}

class TempFile {
   public:
    explicit TempFile(const std::string& tag) : path_(UniqueTempPath(tag)) {}
    TempFile(const TempFile&) = delete;
    TempFile(TempFile&& other) noexcept : path_(std::move(other.path_)) { other.path_.clear(); }
    TempFile& operator=(const TempFile&) = delete;
    TempFile& operator=(TempFile&& other) noexcept {
        if (this != &other) {
            Remove();
            path_ = std::move(other.path_);
            other.path_.clear();
        }
        return *this;
    }
    ~TempFile() { Remove(); }

   public:
    const std::filesystem::path& Path() const { return path_; }

   private:
    void Remove() const {
        if (path_.empty()) {
            return;
        }
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

   private:
    std::filesystem::path path_;
};
