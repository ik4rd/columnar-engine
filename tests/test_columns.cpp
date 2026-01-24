#include <sstream>
#include <stdexcept>
#include <string>

#include "column_int64.h"
#include "column_string.h"
#include "gtest/gtest.h"

TEST(columns, int64_roundtrip) {
    Int64Column values;
    values.AppendFromString("10");
    values.AppendFromString("-5");

    std::stringstream buffer;
    values.WriteTo(buffer);
    const std::string bytes = buffer.str();

    Int64Column read_back;
    std::stringstream in(bytes);
    read_back.ReadFrom(in, 2, bytes.size());

    EXPECT_EQ(read_back.Size(), 2u);
    EXPECT_EQ(read_back.ValueAsString(0), "10");
    EXPECT_EQ(read_back.ValueAsString(1), "-5");
}

TEST(columns, string_roundtrip) {
    StringColumn values;
    values.AppendFromString("alpha");
    values.AppendFromString("line1\nline2");
    values.AppendFromString("be,ta");

    std::stringstream buffer;
    values.WriteTo(buffer);
    const std::string bytes = buffer.str();

    StringColumn read_back;
    std::stringstream in(bytes);
    read_back.ReadFrom(in, 3, bytes.size());

    EXPECT_EQ(read_back.Size(), 3u);
    EXPECT_EQ(read_back.ValueAsString(0), "alpha");
    EXPECT_EQ(read_back.ValueAsString(1), "line1\nline2");
    EXPECT_EQ(read_back.ValueAsString(2), "be,ta");
}

TEST(columns, int64_invalid_value_throws) {
    Int64Column values;
    EXPECT_THROW(values.AppendFromString("not_a_number"), std::runtime_error);
}

TEST(columns, string_out_of_range_throws) {
    StringColumn values;
    values.AppendFromString("alpha");
    EXPECT_THROW(values.ValueAsString(1), std::runtime_error);
}
