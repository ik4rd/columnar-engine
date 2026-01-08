#include <chrono>
#include <filesystem>
#include <random>

#include "fileio.h"
#include "gtest/gtest.h"

namespace {
std::filesystem::path UniqueTempPath(const std::string& tag) {
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
}  // namespace

TEST(fileio, write_and_read_roundtrip) {
    const TempFile temp("fileio_roundtrip");
    const std::vector<uint8_t> payload = {0, 1, 2, 3, 255};

    WriteFileBytes(temp.Path(), payload);
    const auto read_back = ReadFileBytes(temp.Path());

    EXPECT_EQ(read_back, payload);
}

TEST(fileio, append_bytes) {
    const TempFile temp("fileio_append");
    const std::vector<uint8_t> initial = {10, 11};
    std::vector<uint8_t> extra = {12, 13};

    WriteFileBytes(temp.Path(), initial);
    AppendFileBytes(temp.Path(), extra);

    const auto read_back = ReadFileBytes(temp.Path());
    const std::vector<uint8_t> expected = {10, 11, 12, 13};
    EXPECT_EQ(read_back, expected);
}

TEST(fileio, metadata_for_file) {
    const TempFile temp("fileio_meta");
    const std::vector<uint8_t> payload = {42, 43, 44};

    WriteFileBytes(temp.Path(), payload);

    const auto metadata = GetFileMetadata(temp.Path());
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->is_regular);
    EXPECT_FALSE(metadata->is_directory);
    EXPECT_EQ(metadata->size, payload.size());
}

TEST(fileio, metadata_for_missing_file) {
    const auto missing = UniqueTempPath("fileio_missing");
    const auto metadata = GetFileMetadata(missing);

    EXPECT_FALSE(metadata.has_value());
}

TEST(fileio, read_missing_file_throws) {
    auto missing = UniqueTempPath("fileio_missing_read");

    EXPECT_THROW(ReadFileBytes(missing), std::runtime_error);
}

TEST(fileio, file_exists_checks) {
    const TempFile temp("fileio_exists");
    EXPECT_FALSE(FileExists(temp.Path()));

    WriteFileBytes(temp.Path(), std::vector<uint8_t>{1});

    EXPECT_TRUE(FileExists(temp.Path()));
}
