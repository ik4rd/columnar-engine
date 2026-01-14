#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "columnar.h"
#include "csv.h"
#include "fileio.h"
#include "gtest/gtest.h"
#include "metadata.h"
#include "temp_file.h"

TEST(columnar, csv_to_columnar_and_back) {
    const TempFile schema_in("schema_in");
    const TempFile data_in("data_in");
    const TempFile columnar_file("columnar");
    const TempFile schema_out("schema_out");
    const TempFile data_out("data_out");

    const std::vector<std::vector<std::string>> schema_rows = {
        {"a", "int64"},
        {"b", "int64"},
        {"name", "string"},
        {"d", "int64"},
    };

    const std::vector<std::vector<std::string>> data_rows = {
        {"1", "2", "first", "4"},
        {"5", "1", "sec,ond", "2"},
        {"8", "17", "thi\"rd", "2"},
        {"9", "5", "line1\nline2", "10"},
    };

    WriteRows(schema_in.Path(), schema_rows);
    WriteRows(data_in.Path(), data_rows);

    ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 2);
    ConvertColumnarToCsv(columnar_file.Path(), schema_out.Path(), data_out.Path());

    const auto [columns] = ReadSchemaCsv(schema_out.Path());
    ASSERT_EQ(columns.size(), schema_rows.size());
    for (size_t i = 0; i < schema_rows.size(); ++i) {
        EXPECT_EQ(columns[i].name, schema_rows[i][0]);
        EXPECT_EQ((columns[i].type == ColumnType::Int64 ? "int64" : "string"), schema_rows[i][1]);
    }

    const auto data_roundtrip = ReadRows(data_out.Path());
    EXPECT_EQ(data_roundtrip, data_rows);
}

TEST(columnar, metadata_offsets_and_sizes) {
    const TempFile schema_in("schema_meta_in");
    const TempFile data_in("data_meta_in");
    const TempFile columnar_file("columnar_meta");

    const std::vector<std::vector<std::string>> schema_rows = {
        {"id", "int64"},
        {"name", "string"},
    };

    const std::vector<std::vector<std::string>> data_rows = {
        {"1", "a"},
        {"2", "bb"},
        {"3", "ccc"},
    };

    WriteRows(schema_in.Path(), schema_rows);
    WriteRows(data_in.Path(), data_rows);

    ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 2);

    const auto [schema, row_groups] = ReadColumnarMetadata(columnar_file.Path());
    ASSERT_EQ(schema.columns.size(), 2u);
    EXPECT_EQ(schema.columns[0].name, "id");
    EXPECT_EQ(schema.columns[0].type, ColumnType::Int64);
    EXPECT_EQ(schema.columns[1].name, "name");
    EXPECT_EQ(schema.columns[1].type, ColumnType::String);

    ASSERT_EQ(row_groups.size(), 2u);
    EXPECT_EQ(row_groups[0].row_count, 2u);
    EXPECT_EQ(row_groups[1].row_count, 1u);

    ASSERT_EQ(row_groups[0].columns.size(), 2u);
    ASSERT_EQ(row_groups[1].columns.size(), 2u);

    auto string_chunk_size = [&data_rows](const size_t start, const size_t count) -> uint64_t {
        uint64_t total = 0;
        for (size_t i = 0; i < count; ++i) {
            total += sizeof(uint32_t);
            total += data_rows[start + i][1].size();
        }
        return total;
    };

    EXPECT_EQ(row_groups[0].columns[0].size, sizeof(int64_t) * 2u);
    EXPECT_EQ(row_groups[0].columns[1].size, string_chunk_size(0, 2));
    EXPECT_EQ(row_groups[1].columns[0].size, sizeof(int64_t) * 1u);
    EXPECT_EQ(row_groups[1].columns[1].size, string_chunk_size(2, 1));

    uint64_t expected_offset = 0;
    for (const auto& [row_count, columns] : row_groups) {
        for (const auto& [offset, size] : columns) {
            EXPECT_EQ(offset, expected_offset);
            expected_offset += size;
        }
    }

    const auto file_info = GetFileMetadata(columnar_file.Path());
    ASSERT_TRUE(file_info.has_value());
    EXPECT_TRUE(file_info->is_regular);
    EXPECT_LT(expected_offset, file_info->size);
}

TEST(columnar, invalid_int64_throws) {
    const TempFile schema_in("schema_bad_int");
    const TempFile data_in("data_bad_int");
    const TempFile columnar_file("columnar_bad_int");

    WriteRows(schema_in.Path(), {{"value", "int64"}});
    WriteRows(data_in.Path(), {{"not_a_number"}});

    EXPECT_THROW(ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 4), std::runtime_error);
}
