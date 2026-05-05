#include <filesystem>
#include <ios>
#include <string>
#include <vector>

#include "support/error.h"
#include "io/fileio.h"
#include "gtest/gtest.h"
#include "testing/temp_file.h"

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
    const std::vector<uint8_t> extra = {12, 13};

    WriteFileBytes(temp.Path(), initial);
    std::ofstream out = OpenOutputFile(temp.Path(), FileOpenMode::Append);
    out.write(reinterpret_cast<const char*>(extra.data()), extra.size());
    ASSERT_TRUE(out.good());
    out.flush();
    ASSERT_TRUE(out.good());

    const auto read_back = ReadFileBytes(temp.Path());
    const std::vector<uint8_t> expected = {10, 11, 12, 13};
    EXPECT_EQ(read_back, expected);
}

TEST(fileio, metadata_for_file) {
    const TempFile temp("fileio_meta");
    const std::vector<uint8_t> payload = {42, 43, 44};

    WriteFileBytes(temp.Path(), payload);

    const auto metadata = TryGetFileMetadata(temp.Path());
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->is_regular);
    EXPECT_FALSE(metadata->is_directory);
    EXPECT_EQ(metadata->size, payload.size());
}

TEST(fileio, metadata_for_missing_file) {
    const auto missing = UniqueTempPath("fileio_missing");
    const auto metadata = TryGetFileMetadata(missing);

    EXPECT_FALSE(metadata.has_value());
}

TEST(fileio, read_missing_file_throws) {
    const auto missing = UniqueTempPath("fileio_missing_read");

    EXPECT_THROW(ReadFileBytes(missing), Error);
}

TEST(fileio, read_text_roundtrip) {
    const TempFile temp("fileio_text_roundtrip");
    const std::string payload = "SELECT COUNT(*)\nFROM hits\nWHERE UserID = 42;\n";

    std::ofstream out = OpenOutputFile(temp.Path());
    out << payload;
    ASSERT_TRUE(out.good());
    out.flush();
    ASSERT_TRUE(out.good());

    EXPECT_EQ(ReadTextFile(temp.Path()), payload);
}

TEST(fileio, file_exists_checks) {
    const TempFile temp("fileio_exists");
    EXPECT_FALSE(FileExists(temp.Path()));

    WriteFileBytes(temp.Path(), std::vector<uint8_t>{1});

    EXPECT_TRUE(FileExists(temp.Path()));
}

TEST(fileio, ensure_parent_directory_creates_missing_directories) {
    const auto file_path = UniqueTempPath("fileio_nested") / "a" / "b" / "query.sql";
    const auto parent = file_path.parent_path();

    ASSERT_FALSE(std::filesystem::exists(parent));
    EnsureParentDirectory(file_path);
    EXPECT_TRUE(std::filesystem::exists(parent));
    EXPECT_TRUE(std::filesystem::is_directory(parent));
}

TEST(fileio, stream_roundtrip) {
    const TempFile temp("fileio_stream_roundtrip");
    const std::vector<uint8_t> payload = {7, 8, 9, 10, 11};

    std::ofstream out = OpenOutputFile(temp.Path());
    out.write(reinterpret_cast<const char*>(payload.data()), payload.size());
    ASSERT_TRUE(out.good());
    out.flush();
    ASSERT_TRUE(out.good());

    std::ifstream in = OpenInputFile(temp.Path());
    std::vector<uint8_t> read_back(payload.size());
    in.read(reinterpret_cast<char*>(read_back.data()), read_back.size());
    ASSERT_EQ(static_cast<size_t>(in.gcount()), read_back.size());
    EXPECT_EQ(read_back, payload);
}

TEST(fileio, stream_append) {
    const TempFile temp("fileio_stream_append");
    const std::vector<uint8_t> initial = {1, 2};
    const std::vector<uint8_t> extra = {3, 4, 5};

    WriteFileBytes(temp.Path(), initial);

    std::ofstream out = OpenOutputFile(temp.Path(), FileOpenMode::Append);
    out.write(reinterpret_cast<const char*>(extra.data()), extra.size());
    ASSERT_TRUE(out.good());
    out.flush();
    ASSERT_TRUE(out.good());

    const auto read_back = ReadFileBytes(temp.Path());
    const std::vector<uint8_t> expected = {1, 2, 3, 4, 5};
    EXPECT_EQ(read_back, expected);
}

TEST(fileio, stream_seek_and_tell) {
    const TempFile temp("fileio_stream_seek");
    const std::vector<uint8_t> payload = {100, 101, 102, 103};

    WriteFileBytes(temp.Path(), payload);

    std::ifstream in = OpenInputFile(temp.Path());
    EXPECT_EQ(in.tellg(), std::streampos(0));
    in.seekg(std::streampos(2));
    ASSERT_TRUE(in.good());
    EXPECT_EQ(in.tellg(), std::streampos(2));

    uint8_t value = 0;
    in.read(reinterpret_cast<char*>(&value), 1);
    ASSERT_EQ(in.gcount(), 1);
    EXPECT_EQ(value, payload[2]);
}

TEST(fileio, stream_open_missing_throws) {
    const auto missing = UniqueTempPath("fileio_stream_missing");

    EXPECT_THROW({ std::ifstream in = OpenInputFile(missing); }, Error);
}
