#pragma once

#include <chrono>
#include <filesystem>
#include <random>
#include <string>
#include <system_error>

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

    ~TempFile() {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

    const std::filesystem::path& Path() const { return path_; }

   private:
    std::filesystem::path path_;
};
