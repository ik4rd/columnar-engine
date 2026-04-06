#include <sstream>
#include <string>
#include <vector>

#include "csv.h"
#include "gtest/gtest.h"

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
