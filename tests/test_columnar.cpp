#include <filesystem>
#include <sstream>
#include <string>
#include <vector>

#include "common/error.h"
#include "common/parsing.h"
#include "convert/csv_columnar.h"
#include "gtest/gtest.h"
#include "io/columnar_batch.h"
#include "io/compression.h"
#include "io/csv.h"
#include "io/file.h"
#include "io/stream.h"
#include "model/metadata.h"
#include "model/schema.h"
#include "model/schema_csv.h"
#include "testing/temp_file.h"

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

    EXPECT_EQ(row_groups[0].columns[0].compressed_size, sizeof(int64_t) * 2u);
    EXPECT_EQ(row_groups[0].columns[0].uncompressed_size, sizeof(int64_t) * 2u);
    EXPECT_EQ(row_groups[0].columns[0].compression, Compression::None);
    EXPECT_EQ(row_groups[0].columns[1].compressed_size, string_chunk_size(0, 2));
    EXPECT_EQ(row_groups[0].columns[1].uncompressed_size, string_chunk_size(0, 2));
    EXPECT_EQ(row_groups[0].columns[1].compression, Compression::None);
    EXPECT_EQ(row_groups[1].columns[0].compressed_size, sizeof(int64_t) * 1u);
    EXPECT_EQ(row_groups[1].columns[0].uncompressed_size, sizeof(int64_t) * 1u);
    EXPECT_EQ(row_groups[1].columns[0].compression, Compression::None);
    EXPECT_EQ(row_groups[1].columns[1].compressed_size, string_chunk_size(2, 1));
    EXPECT_EQ(row_groups[1].columns[1].uncompressed_size, string_chunk_size(2, 1));
    EXPECT_EQ(row_groups[1].columns[1].compression, Compression::None);
    EXPECT_TRUE(row_groups[0].columns[0].has_min_max);
    EXPECT_EQ(row_groups[0].columns[0].min_value, 1);
    EXPECT_EQ(row_groups[0].columns[0].max_value, 2);
    EXPECT_FALSE(row_groups[0].columns[1].has_min_max);
    EXPECT_TRUE(row_groups[1].columns[0].has_min_max);
    EXPECT_EQ(row_groups[1].columns[0].min_value, 3);
    EXPECT_EQ(row_groups[1].columns[0].max_value, 3);
    EXPECT_FALSE(row_groups[1].columns[1].has_min_max);

    uint64_t expected_offset = 0;
    for (const auto& row_group : row_groups) {
        for (const auto& column : row_group.columns) {
            EXPECT_EQ(column.offset, expected_offset);
            expected_offset += column.compressed_size;
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

    EXPECT_THROW(ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 4), Error);
}

TEST(columnar, mixed_supported_types_roundtrip) {
    const TempFile schema_in("schema_mixed_in");
    const TempFile data_in("data_mixed_in");
    const TempFile columnar_file("columnar_mixed");
    const TempFile schema_out("schema_mixed_out");
    const TempFile data_out("data_mixed_out");

    const std::vector<std::vector<std::string>> schema_rows = {
        {"flag", "bool"},   {"small", "int16"},     {"medium", "int32"},         {"big", "int64"},  {"huge", "int128"},
        {"name", "string"}, {"birth_date", "date"}, {"created_at", "timestamp"}, {"grade", "char"},
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

    EXPECT_EQ(ReadRows(schema_out.Path()), (std::vector<std::vector<std::string>>{
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

TEST(columnar, metadata_records_minmax_stats_for_date_columns) {
    const TempFile schema_in("schema_date_stats");
    const TempFile data_in("data_date_stats");
    const TempFile columnar_file("columnar_date_stats");

    WriteRows(schema_in.Path(), {{"event_date", "date"}});
    WriteRows(data_in.Path(), {{"2024-01-05"}, {"2024-01-03"}, {"2024-01-09"}, {"2024-01-01"}});

    ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 2);

    ColumnarBatchReader reader(columnar_file.Path());
    const auto& row_groups = reader.GetMetadata().row_groups;
    ASSERT_EQ(row_groups.size(), 2u);
    ASSERT_EQ(row_groups[0].columns.size(), 1u);
    ASSERT_EQ(row_groups[1].columns.size(), 1u);

    EXPECT_TRUE(row_groups[0].columns[0].has_min_max);
    EXPECT_EQ(DateToString(static_cast<int32_t>(row_groups[0].columns[0].min_value)), "2024-01-03");
    EXPECT_EQ(DateToString(static_cast<int32_t>(row_groups[0].columns[0].max_value)), "2024-01-05");

    EXPECT_TRUE(row_groups[1].columns[0].has_min_max);
    EXPECT_EQ(DateToString(static_cast<int32_t>(row_groups[1].columns[0].min_value)), "2024-01-01");
    EXPECT_EQ(DateToString(static_cast<int32_t>(row_groups[1].columns[0].max_value)), "2024-01-09");
}

TEST(columnar, lz4_compressed_roundtrip) {
    const TempFile schema_in("schema_lz4_in");
    const TempFile data_in("data_lz4_in");
    const TempFile columnar_file("columnar_lz4");
    const TempFile data_out("data_lz4_out");
    const TempFile schema_out("schema_lz4_out");

    WriteRows(schema_in.Path(), {{"id", "int64"}, {"payload", "string"}});

    std::vector<std::vector<std::string>> data_rows;
    for (int i = 0; i < 256; ++i) {
        data_rows.push_back({std::to_string(i), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"});
    }
    WriteRows(data_in.Path(), data_rows);

    ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 256, Compression::Lz4);
    ConvertColumnarToCsv(columnar_file.Path(), schema_out.Path(), data_out.Path());

    ColumnarBatchReader reader(columnar_file.Path());
    const auto& row_group = reader.GetMetadata().row_groups.at(0);
    ASSERT_EQ(row_group.columns.size(), 2u);
    EXPECT_EQ(ReadRows(data_out.Path()), data_rows);
    EXPECT_EQ(ReadRows(schema_out.Path()),
              (std::vector<std::vector<std::string>>{{"id", "int64"}, {"payload", "string"}}));
    EXPECT_EQ(row_group.columns[1].compression, Compression::Lz4);
    EXPECT_LT(row_group.columns[1].compressed_size, row_group.columns[1].uncompressed_size);
}

TEST(columnar, lz4_falls_back_to_none_when_chunk_does_not_shrink) {
    const TempFile schema_in("schema_lz4_fallback_in");
    const TempFile data_in("data_lz4_fallback_in");
    const TempFile columnar_file("columnar_lz4_fallback");

    WriteRows(schema_in.Path(), {{"value", "int64"}});
    WriteRows(data_in.Path(), {{"42"}});

    ConvertCsvToColumnar(schema_in.Path(), data_in.Path(), columnar_file.Path(), 1, Compression::Lz4);

    const ColumnarBatchReader reader(columnar_file.Path());
    const auto& chunk = reader.GetMetadata().row_groups.at(0).columns.at(0);
    EXPECT_EQ(chunk.compression, Compression::None);
    EXPECT_EQ(chunk.compressed_size, sizeof(int64_t));
    EXPECT_EQ(chunk.uncompressed_size, sizeof(int64_t));
}

TEST(columnar, read_legacy_metadata_without_compression_fields) {
    std::ostringstream out(std::ios::binary);

    WriteStream<uint32_t>(out, 1);
    WriteStream<uint32_t>(out, 2);
    WriteBytes(out, "id");
    WriteStream<uint8_t>(out, static_cast<uint8_t>(ColumnType::Int64));
    WriteStream<uint32_t>(out, 1);
    WriteStream<uint32_t>(out, 3);
    WriteStream<uint32_t>(out, 1);
    WriteStream<uint64_t>(out, 10);
    WriteStream<uint64_t>(out, 24);
    WriteStream<uint8_t>(out, 1);
    WriteStream<Int128>(out, 7);
    WriteStream<Int128>(out, 9);

    std::istringstream in(out.str(), std::ios::binary);
    const ColumnarMetadata metadata = ReadMetadata(in);

    ASSERT_EQ(metadata.schema.columns.size(), 1u);
    ASSERT_EQ(metadata.row_groups.size(), 1u);
    const auto& chunk = metadata.row_groups[0].columns[0];
    EXPECT_EQ(chunk.offset, 10u);
    EXPECT_EQ(chunk.compressed_size, 24u);
    EXPECT_EQ(chunk.uncompressed_size, 24u);
    EXPECT_EQ(chunk.compression, Compression::None);
    EXPECT_TRUE(chunk.has_min_max);
    EXPECT_EQ(chunk.min_value, 7);
    EXPECT_EQ(chunk.max_value, 9);
}
