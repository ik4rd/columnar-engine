#include <filesystem>
#include <ios>
#include <stdexcept>
#include <string>
#include <vector>

#include "fileio.h"
#include "gtest/gtest.h"
#include "temp_file.h"

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
    const auto missing = UniqueTempPath("fileio_missing_read");

    EXPECT_THROW(ReadFileBytes(missing), std::runtime_error);
}

TEST(fileio, file_exists_checks) {
    const TempFile temp("fileio_exists");
    EXPECT_FALSE(FileExists(temp.Path()));

    WriteFileBytes(temp.Path(), std::vector<uint8_t>{1});

    EXPECT_TRUE(FileExists(temp.Path()));
}

TEST(fileio, stream_roundtrip) {
    const TempFile temp("fileio_stream_roundtrip");
    const std::vector<uint8_t> payload = {7, 8, 9, 10, 11};

    FileWriter writer(temp.Path());
    auto& out = writer.Stream();
    out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    ASSERT_TRUE(out.good());
    writer.Flush();
    writer.Close();

    FileReader reader(temp.Path());
    auto& in = reader.Stream();
    std::vector<uint8_t> read_back(payload.size());
    in.read(reinterpret_cast<char*>(read_back.data()), static_cast<std::streamsize>(read_back.size()));
    ASSERT_EQ(static_cast<size_t>(in.gcount()), read_back.size());
    EXPECT_EQ(read_back, payload);
}

TEST(fileio, stream_append) {
    const TempFile temp("fileio_stream_append");
    const std::vector<uint8_t> initial = {1, 2};
    const std::vector<uint8_t> extra = {3, 4, 5};

    WriteFileBytes(temp.Path(), initial);

    FileWriter writer(temp.Path(), FileOpenMode::Append);
    auto& out = writer.Stream();
    out.write(reinterpret_cast<const char*>(extra.data()), static_cast<std::streamsize>(extra.size()));
    ASSERT_TRUE(out.good());
    writer.Flush();
    writer.Close();

    const auto read_back = ReadFileBytes(temp.Path());
    const std::vector<uint8_t> expected = {1, 2, 3, 4, 5};
    EXPECT_EQ(read_back, expected);
}

TEST(fileio, stream_seek_and_tell) {
    const TempFile temp("fileio_stream_seek");
    const std::vector<uint8_t> payload = {100, 101, 102, 103};

    WriteFileBytes(temp.Path(), payload);

    FileReader reader(temp.Path());
    EXPECT_EQ(reader.Tell(), std::streampos(0));
    reader.Seek(std::streampos(2));
    EXPECT_EQ(reader.Tell(), std::streampos(2));

    auto& in = reader.Stream();
    uint8_t value = 0;
    in.read(reinterpret_cast<char*>(&value), 1);
    ASSERT_EQ(in.gcount(), 1);
    EXPECT_EQ(value, payload[2]);
}

TEST(fileio, stream_open_missing_throws) {
    const auto missing = UniqueTempPath("fileio_stream_missing");

    EXPECT_THROW({ FileReader reader(missing); }, std::runtime_error);
}
