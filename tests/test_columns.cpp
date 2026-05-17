#include <sstream>
#include <string>
#include <vector>

#include "model/column.h"
#include "model/column_int64.h"
#include "model/column_string.h"
#include "common/error.h"
#include "gtest/gtest.h"
#include "common/parsing.h"

static void ExpectColumnRoundtrip(const ColumnType type, const std::vector<std::string>& values,
                                  const std::vector<std::string>& expected_values = {}) {
    auto column = CreateColumn(type);
    for (const auto& value : values) {
        column->AppendFromString(value);
    }

    std::stringstream buffer;
    column->WriteTo(buffer);
    const std::string bytes = buffer.str();

    auto read_back = CreateColumn(type);
    std::stringstream in(bytes);
    read_back->ReadFrom(in, values.size(), bytes.size());

    ASSERT_EQ(read_back->Size(), values.size());
    const auto& expected = expected_values.empty() ? values : expected_values;
    ASSERT_EQ(expected.size(), values.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(read_back->ValueAsString(i), expected[i]);
    }
}

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
    EXPECT_THROW(values.AppendFromString("not_a_number"), Error);
}

TEST(columns, string_out_of_range_throws) {
    StringColumn values;
    values.AppendFromString("alpha");
    EXPECT_THROW(values.ValueAsString(1), Error);
}

TEST(columns, supported_scalar_types_roundtrip) {
    ExpectColumnRoundtrip(ColumnType::Boolean, {"true", "false", "1"}, {"true", "false", "true"});
    ExpectColumnRoundtrip(ColumnType::Int16, {"-32768", "0", "32767"});
    ExpectColumnRoundtrip(ColumnType::Int32, {"-2147483648", "7", "2147483647"});
    ExpectColumnRoundtrip(
        ColumnType::Int128,
        {"-170141183460469231731687303715884105728", "0", "170141183460469231731687303715884105727"});
    ExpectColumnRoundtrip(ColumnType::Date, {"1970-01-01", "2024-02-29", "2030-12-31"});
    ExpectColumnRoundtrip(
        ColumnType::Timestamp,
        {"1970-01-01 00:00:00", "2024-02-29 12:34:56.123456", "2030-12-31T23:59:59.5"},
        {"1970-01-01 00:00:00", "2024-02-29 12:34:56.123456", "2030-12-31 23:59:59.5"});
    ExpectColumnRoundtrip(ColumnType::Character, {"A", ",", "\n"});
}

TEST(columns, scalar_invalid_values_throw) {
    auto boolean = CreateColumn(ColumnType::Boolean);
    EXPECT_THROW(boolean->AppendFromString("maybe"), Error);

    auto int16 = CreateColumn(ColumnType::Int16);
    EXPECT_THROW(int16->AppendFromString("40000"), Error);

    auto int32 = CreateColumn(ColumnType::Int32);
    EXPECT_THROW(int32->AppendFromString("2147483648"), Error);

    auto int128 = CreateColumn(ColumnType::Int128);
    EXPECT_THROW(int128->AppendFromString("170141183460469231731687303715884105728"), Error);

    auto date = CreateColumn(ColumnType::Date);
    EXPECT_THROW(date->AppendFromString("2024-02-30"), Error);

    auto timestamp = CreateColumn(ColumnType::Timestamp);
    EXPECT_THROW(timestamp->AppendFromString("2024-02-29 24:00:00"), Error);

    auto character = CreateColumn(ColumnType::Character);
    EXPECT_THROW(character->AppendFromString("ab"), Error);
}

TEST(columns, type_names_roundtrip) {
    const std::vector<ColumnType> types = {
        ColumnType::Boolean,
        ColumnType::Int16,
        ColumnType::Int32,
        ColumnType::Int64,
        ColumnType::Int128,
        ColumnType::String,
        ColumnType::Date,
        ColumnType::Timestamp,
        ColumnType::Character,
    };

    for (const auto type : types) {
        EXPECT_EQ(ParseColumnType(ColumnTypeToString(type)), type);
    }
}
