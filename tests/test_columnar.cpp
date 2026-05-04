#include <filesystem>
#include <string>
#include <vector>

#include "columnar_batch_io.h"
#include "csv_columnar.h"
#include "csv.h"
#include "error.h"
#include "fileio.h"
#include "gtest/gtest.h"
#include "parsing.h"
#include "schema.h"
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
        EXPECT_EQ(ColumnTypeToString(columns[i].type), schema_rows[i][1]);
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

    ColumnarBatchReader reader(columnar_file.Path());
    const auto& [schema, row_groups] = reader.GetMetadata();
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

    const auto file_info = TryGetFileMetadata(columnar_file.Path());
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

    EXPECT_THROW(ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 4), Error);
}

TEST(columnar, mixed_supported_types_roundtrip) {
    const TempFile schema_in("schema_mixed_in");
    const TempFile data_in("data_mixed_in");
    const TempFile columnar_file("columnar_mixed");
    const TempFile schema_out("schema_mixed_out");
    const TempFile data_out("data_mixed_out");

    const std::vector<std::vector<std::string>> schema_rows = {
        {"flag", "bool"},
        {"small", "int16"},
        {"medium", "int32"},
        {"big", "int64"},
        {"huge", "int128"},
        {"name", "string"},
        {"birth_date", "date"},
        {"created_at", "timestamp"},
        {"grade", "char"},
    };

    const std::vector<std::vector<std::string>> data_rows = {
        {"true", "-7", "12345", "9999999999", "170141183460469231731687303715884105727", "alpha", "2024-02-29",
         "2024-02-29 12:34:56.123456", "A"},
        {"false", "8", "-54321", "-42", "-170141183460469231731687303715884105728", "be,ta", "1970-01-01",
         "1970-01-01 00:00:00", "Z"},
    };

    WriteRows(schema_in.Path(), schema_rows);
    WriteRows(data_in.Path(), data_rows);

    ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 1);
    ConvertColumnarToCsv(columnar_file.Path(), schema_out.Path(), data_out.Path());

    EXPECT_EQ(ReadRows(schema_out.Path()), schema_rows);
    EXPECT_EQ(ReadRows(data_out.Path()), data_rows);
}

TEST(columnar, infer_schema_from_csv_is_convert_compatible) {
    const TempFile data_in("data_infer_in");
    const TempFile schema_out("schema_infer_out");
    const TempFile columnar_file("columnar_infer");
    const TempFile data_out("data_infer_out");

    const std::vector<std::vector<std::string>> data_rows = {
        {"true", "-7", "12345", "9999999999", "170141183460469231731687303715884105727", "alpha", "2024-02-29",
         "2024-02-29 12:34:56.123456", "A"},
        {"false", "8", "-54321", "-42", "-170141183460469231731687303715884105728", "be,ta", "1970-01-01",
         "1970-01-01 00:00:00", "Z"},
    };

    WriteRows(data_in.Path(), data_rows);
    WriteSchemaCsv(schema_out.Path(), InferSchemaCsv(data_in.Path()));

    EXPECT_EQ(ReadRows(schema_out.Path()),
              (std::vector<std::vector<std::string>>{
                  {"column_1", "bool"},
                  {"column_2", "int16"},
                  {"column_3", "int32"},
                  {"column_4", "int64"},
                  {"column_5", "int128"},
                  {"column_6", "string"},
                  {"column_7", "date"},
                  {"column_8", "timestamp"},
                  {"column_9", "char"},
              }));

    ConvertCsvToColumnar(schema_out.Path(), data_in.Path(), columnar_file.Path(), 1);
    ConvertColumnarToCsv(columnar_file.Path(), schema_out.Path(), data_out.Path());

    EXPECT_EQ(ReadRows(data_out.Path()), data_rows);
}

TEST(columnar, schema_rows_with_trailing_empty_columns_are_accepted) {
    const TempFile schema_in("schema_trailing_empty");
    const TempFile data_in("data_trailing_empty");
    const TempFile columnar_file("columnar_trailing_empty");

    WriteRows(schema_in.Path(), {{"id", "int64", ""}, {"name", "string", ""}});
    WriteRows(data_in.Path(), {{"1", "alpha"}, {"2", "beta"}});

    EXPECT_NO_THROW(ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 2));
}
