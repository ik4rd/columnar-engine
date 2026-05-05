#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "io/csv.h"
#include "gtest/gtest.h"

static_assert(!std::is_copy_constructible_v<CsvReader>);
static_assert(!std::is_copy_assignable_v<CsvReader>);
static_assert(std::is_move_constructible_v<CsvReader>);
static_assert(std::is_move_assignable_v<CsvReader>);
static_assert(!std::is_copy_constructible_v<CsvWriter>);
static_assert(!std::is_copy_assignable_v<CsvWriter>);
static_assert(std::is_move_constructible_v<CsvWriter>);
static_assert(std::is_move_assignable_v<CsvWriter>);

TEST(csv, read_write_roundtrip_with_quotes) {
    std::stringstream buffer;
    CsvWriter writer(buffer);
    CsvReader reader(buffer);

    writer.WriteRow({"1", "2,3", "he\"llo", "line1\nline2"});
    writer.WriteRow({"", "plain"});

    std::vector<std::string> row;
    ASSERT_TRUE(reader.ReadRow(row));
    EXPECT_EQ(row, (std::vector<std::string>{"1", "2,3", "he\"llo", "line1\nline2"}));

    ASSERT_TRUE(reader.ReadRow(row));
    EXPECT_EQ(row, (std::vector<std::string>{"", "plain"}));

    EXPECT_FALSE(reader.ReadRow(row));
}

TEST(csv, handles_crlf_newlines) {
    std::istringstream input("a,b\r\nc,d\r\n");
    CsvReader reader(input);
    std::vector<std::string> row;

    ASSERT_TRUE(reader.ReadRow(row));
    EXPECT_EQ(row, (std::vector<std::string>{"a", "b"}));

    ASSERT_TRUE(reader.ReadRow(row));
    EXPECT_EQ(row, (std::vector<std::string>{"c", "d"}));

    EXPECT_FALSE(reader.ReadRow(row));
}

TEST(csv, moved_reader_and_writer_remain_usable) {
    std::stringstream buffer;
    CsvWriter writer(buffer);
    CsvWriter moved_writer(std::move(writer));
    moved_writer.WriteRow({"1", "value"});
    moved_writer.Flush();

    std::istringstream input(buffer.str());
    CsvReader reader(input);
    CsvReader moved_reader(std::move(reader));

    std::vector<std::string> row;
    ASSERT_TRUE(moved_reader.ReadRow(row));
    EXPECT_EQ(row, (std::vector<std::string>{"1", "value"}));
    EXPECT_FALSE(moved_reader.ReadRow(row));
}
