#include <algorithm>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "convert/csv_columnar.h"
#include "executor/executor.h"
#include "gtest/gtest.h"
#include "io/columnar_batch.h"
#include "io/csv.h"
#include "io/file.h"
#include "model/metadata.h"
#include "testing/executor_test_utils.h"
#include "testing/temp_file.h"

static Batch BuildHitsTable(const std::string_view query) {
    const TempFile schema_file("executor_schema");
    const TempFile data_file("executor_data");
    const TempFile columnar_file("executor_columnar");

    WriteRows(schema_file.Path(), {
                                      {"AdvEngineID", "int64"},
                                      {"ResolutionWidth", "int32"},
                                      {"UserID", "int64"},
                                      {"RegionID", "int32"},
                                      {"MobilePhone", "bool"},
                                      {"MobilePhoneModel", "string"},
                                      {"SearchEngineID", "int32"},
                                      {"SearchPhrase", "string"},
                                      {"EventDate", "date"},
                                  });

    WriteRows(data_file.Path(), {
                                    {"0", "100", "1", "10", "false", "", "1", "alpha", "2024-01-05"},
                                    {"7", "200", "2", "20", "true", "iPhone", "1", "beta", "2024-01-03"},
                                    {"3", "301", "2", "10", "true", "Pixel", "2", "alpha", "2024-01-07"},
                                    {"7", "500", "5", "20", "true", "iPhone", "1", "gamma", "2024-01-01"},
                                    {"1", "400", "11", "30", "false", "Nokia", "2", "delta", "2024-01-09"},
                                });

    ConvertCsvToColumnar(schema_file.Path(), data_file.Path(), columnar_file.Path(), 2);

    Executor executor;
    executor.RegisterTable("hits", columnar_file.Path());

    auto result = executor.Execute(query);
    if (!result.has_value()) {
        throw result.error();
    }

    return std::move(result.value());
}

TEST(executor, usage_example_register_table_and_execute_query) {
    const Batch batch = BuildHitsTable("SELECT COUNT(*) FROM hits;");
    EXPECT_EQ(BatchColumnNames(batch), std::vector<std::string>{"COUNT(*)"});
    EXPECT_EQ(SingleRowValues(batch), std::vector<std::string>{"5"});
}

TEST(executor, groups_rows_and_orders_by_aggregate_descending) {
    const Batch batch = BuildHitsTable(
        "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;");

    EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"AdvEngineID", "COUNT(*)"}));
    auto actual_rows = BatchRows(batch);
    auto expected_rows = std::vector<std::vector<std::string>>{
        {"7", "2"},
        {"1", "1"},
        {"3", "1"},
    };
    std::ranges::sort(actual_rows.begin() + 1, actual_rows.end());
    std::ranges::sort(expected_rows.begin() + 1, expected_rows.end());
    EXPECT_EQ(actual_rows, expected_rows);
}

TEST(executor, encodes_multicolumn_group_keys_with_length_prefix) {
    const TempFile schema_file("executor_schema");
    const TempFile data_file("executor_data");
    const TempFile columnar_file("executor_columnar");

    WriteRows(schema_file.Path(), {
                                      {"KeyLeft", "string"},
                                      {"KeyRight", "string"},
                                      {"Value", "int64"},
                                  });
    WriteRows(data_file.Path(), {
                                    {"ab", "c", "1"},
                                    {"a", "bc", "2"},
                                    {"ab", "c", "3"},
                                    {"a|", "bc", "4"},
                                    {"a|", "bc", "5"},
                                });
    ConvertCsvToColumnar(schema_file.Path(), data_file.Path(), columnar_file.Path(), 2);

    Executor executor;
    executor.RegisterTable("events", columnar_file.Path());

    auto result = executor.Execute("SELECT KeyLeft, KeyRight, COUNT(*) FROM events GROUP BY KeyLeft, KeyRight;");
    ASSERT_TRUE(result.has_value()) << result.error().what();

    EXPECT_EQ(BatchColumnNames(result.value()), (std::vector<std::string>{"KeyLeft", "KeyRight", "COUNT(*)"}));
    auto actual_rows = BatchRows(result.value());
    auto expected_rows = std::vector<std::vector<std::string>>{
        {"a", "bc", "1"},
        {"a|", "bc", "2"},
        {"ab", "c", "2"},
    };
    std::ranges::sort(actual_rows);
    std::ranges::sort(expected_rows);
    EXPECT_EQ(actual_rows, expected_rows);
}

TEST(executor, supports_from_with_as_alias_and_qualified_columns) {
    const Batch batch = BuildHitsTable(
        "SELECT h.AdvEngineID, COUNT(*) FROM hits AS h WHERE h.AdvEngineID <> 0 GROUP BY h.AdvEngineID ORDER BY "
        "COUNT(*) DESC;");

    EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"AdvEngineID", "COUNT(*)"}));
    auto actual_rows = BatchRows(batch);
    auto expected_rows = std::vector<std::vector<std::string>>{
        {"7", "2"},
        {"1", "1"},
        {"3", "1"},
    };
    std::ranges::sort(actual_rows.begin() + 1, actual_rows.end());
    std::ranges::sort(expected_rows.begin() + 1, expected_rows.end());
    EXPECT_EQ(actual_rows, expected_rows);
}

TEST(executor, supports_from_with_bare_alias) {
    const Batch batch = BuildHitsTable(
        "SELECT h.AdvEngineID, COUNT(*) FROM hits h WHERE h.AdvEngineID <> 0 GROUP BY h.AdvEngineID ORDER BY "
        "COUNT(*) DESC;");

    EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"AdvEngineID", "COUNT(*)"}));
    auto actual_rows = BatchRows(batch);
    auto expected_rows = std::vector<std::vector<std::string>>{
        {"7", "2"},
        {"1", "1"},
        {"3", "1"},
    };
    std::ranges::sort(actual_rows.begin() + 1, actual_rows.end());
    std::ranges::sort(expected_rows.begin() + 1, expected_rows.end());
    EXPECT_EQ(actual_rows, expected_rows);
}

TEST(executor, supports_column_to_column_filter) {
    const Batch batch = BuildHitsTable("SELECT COUNT(*) FROM hits h WHERE h.AdvEngineID > h.SearchEngineID;");

    EXPECT_EQ(BatchColumnNames(batch), std::vector<std::string>{"COUNT(*)"});
    EXPECT_EQ(SingleRowValues(batch), std::vector<std::string>{"3"});
}

TEST(executor, supports_select_alias_order_by_alias_and_limit) {
    const Batch batch = BuildHitsTable(
        "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 2;");

    EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"RegionID", "u"}));
    auto actual_rows = BatchRows(batch);
    auto expected_rows = std::vector<std::vector<std::string>>{
        {"10", "2"},
        {"20", "2"},
    };
    std::ranges::sort(actual_rows);
    std::ranges::sort(expected_rows);
    EXPECT_EQ(actual_rows, expected_rows);
}

TEST(executor, supports_order_by_limit_top_k_in_result_order) {
    const Batch batch =
        BuildHitsTable("SELECT RegionID, COUNT(*) FROM hits GROUP BY RegionID ORDER BY RegionID ASC LIMIT 2;");

    EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"RegionID", "COUNT(*)"}));
    EXPECT_EQ(BatchRows(batch), (std::vector<std::vector<std::string>>{
                                    {"10", "2"},
                                    {"20", "2"},
                                }));
}

TEST(executor, supports_select_star_order_by_non_first_column) {
    const Batch batch = BuildHitsTable("SELECT * FROM hits ORDER BY EventDate ASC LIMIT 3;");

    EXPECT_EQ(BatchColumnNames(batch),
              (std::vector<std::string>{"AdvEngineID", "ResolutionWidth", "UserID", "RegionID", "MobilePhone",
                                        "MobilePhoneModel", "SearchEngineID", "SearchPhrase", "EventDate"}));
    EXPECT_EQ(BatchRows(batch), (std::vector<std::vector<std::string>>{
                                    {"7", "500", "5", "20", "true", "iPhone", "1", "gamma", "2024-01-01"},
                                    {"7", "200", "2", "20", "true", "iPhone", "1", "beta", "2024-01-03"},
                                    {"0", "100", "1", "10", "false", "", "1", "alpha", "2024-01-05"},
                                }));
}

TEST(executor, supports_plain_select_order_by_hidden_typed_column) {
    const Batch batch =
        BuildHitsTable("SELECT SearchPhrase FROM hits WHERE SearchPhrase != '' ORDER BY EventDate ASC LIMIT 3;");

    EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"SearchPhrase", "EventDate"}));
    EXPECT_EQ(BatchRows(batch), (std::vector<std::vector<std::string>>{
                                    {"gamma", "2024-01-01"},
                                    {"beta", "2024-01-03"},
                                    {"alpha", "2024-01-05"},
                                }));
}

TEST(executor, supports_clickbench_star_order_by_projected_column) {
    const TempFile schema_file("executor_schema");
    const TempFile data_file("executor_data");
    const TempFile columnar_file("executor_columnar");

    WriteRows(schema_file.Path(), {
                                      {"WatchID", "int64"},
                                      {"EventTime", "timestamp"},
                                      {"URL", "string"},
                                      {"Title", "string"},
                                      {"SearchPhrase", "string"},
                                  });
    WriteRows(data_file.Path(), {
                                    {"1", "2024-01-05 00:00:00", "http://example.com", "other", "skip"},
                                    {"2", "2024-01-03 00:00:00", "http://google.com/a", "first", "a"},
                                    {"3", "2024-01-01 00:00:00", "https://google.com/b", "second", "b"},
                                    {"4", "2024-01-04 00:00:00", "http://google.com/c", "third", "c"},
                                });
    ConvertCsvToColumnar(schema_file.Path(), data_file.Path(), columnar_file.Path(), 2);

    Executor executor;
    executor.RegisterTable("hits", columnar_file.Path());

    auto result = executor.Execute("SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime ASC LIMIT 2;");
    ASSERT_TRUE(result.has_value()) << result.error().what();

    EXPECT_EQ(BatchColumnNames(result.value()),
              (std::vector<std::string>{"WatchID", "EventTime", "URL", "Title"}));
    EXPECT_EQ(BatchRows(result.value()), (std::vector<std::vector<std::string>>{
                                             {"3", "2024-01-01 00:00:00", "https://google.com/b", "second"},
                                             {"2", "2024-01-03 00:00:00", "http://google.com/a", "first"},
                                         }));
}

TEST(executor, supports_multiple_aggregates_with_alias_and_limit) {
    const Batch batch = BuildHitsTable(
        "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) "
        "FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 2;");

    EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"RegionID", "SUM(AdvEngineID)", "c",
                                                                 "AVG(ResolutionWidth)", "COUNT(DISTINCT UserID)"}));
    auto actual_rows = BatchRows(batch);
    auto expected_rows = std::vector<std::vector<std::string>>{
        {"10", "3", "2", "200", "2"},
        {"20", "14", "2", "350", "2"},
    };
    std::ranges::sort(actual_rows);
    std::ranges::sort(expected_rows);
    EXPECT_EQ(actual_rows, expected_rows);
}

TEST(executor, supports_top_k_group_by_multiple_numeric_keys_with_compact_aggregates) {
    const TempFile schema_file("executor_schema");
    const TempFile data_file("executor_data");
    const TempFile columnar_file("executor_columnar");

    WriteRows(schema_file.Path(), {
                                      {"WatchID", "int64"},
                                      {"ClientIP", "int32"},
                                      {"IsRefresh", "int16"},
                                      {"ResolutionWidth", "int16"},
                                  });
    WriteRows(data_file.Path(), {
                                    {"10", "1", "1", "100"},
                                    {"10", "1", "0", "200"},
                                    {"10", "1", "1", "300"},
                                    {"11", "1", "1", "400"},
                                    {"11", "1", "0", "600"},
                                    {"10", "2", "1", "800"},
                                });
    ConvertCsvToColumnar(schema_file.Path(), data_file.Path(), columnar_file.Path(), 2);

    Executor executor;
    executor.RegisterTable("hits", columnar_file.Path());

    auto result = executor.Execute(
        "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, "
        "ClientIP ORDER BY c DESC LIMIT 2;");
    ASSERT_TRUE(result.has_value()) << result.error().what();

    EXPECT_EQ(BatchColumnNames(result.value()),
              (std::vector<std::string>{"WatchID", "ClientIP", "c", "SUM(IsRefresh)", "AVG(ResolutionWidth)"}));
    EXPECT_EQ(BatchRows(result.value()), (std::vector<std::vector<std::string>>{
                                             {"10", "1", "3", "2", "200"},
                                             {"11", "1", "2", "1", "500"},
                                         }));
}

TEST(executor, executes_basic_aggregate_queries) {
    {
        const Batch batch = BuildHitsTable("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;");
        EXPECT_EQ(BatchColumnNames(batch), std::vector<std::string>{"COUNT(*)"});
        EXPECT_EQ(SingleRowValues(batch), std::vector<std::string>{"4"});
    }

    {
        const Batch batch = BuildHitsTable("SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch),
                  (std::vector<std::string>{"SUM(AdvEngineID)", "COUNT(*)", "AVG(ResolutionWidth)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"18", "5", "300"}));
    }

    {
        const Batch batch = BuildHitsTable("SELECT AVG(UserID) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), std::vector<std::string>{"AVG(UserID)"});
        EXPECT_EQ(SingleRowValues(batch), std::vector<std::string>{"4"});
    }

    {
        const Batch batch = BuildHitsTable("SELECT COUNT(DISTINCT UserID) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), std::vector<std::string>{"COUNT(DISTINCT UserID)"});
        EXPECT_EQ(SingleRowValues(batch), std::vector<std::string>{"4"});
    }

    {
        const Batch batch = BuildHitsTable("SELECT COUNT(DISTINCT SearchPhrase) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), std::vector<std::string>{"COUNT(DISTINCT SearchPhrase)"});
        EXPECT_EQ(SingleRowValues(batch), std::vector<std::string>{"4"});
    }

    {
        const Batch batch = BuildHitsTable("SELECT MIN(EventDate), MAX(EventDate) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"MIN(EventDate)", "MAX(EventDate)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"2024-01-01", "2024-01-09"}));
    }

    {
        const Batch batch = BuildHitsTable("select sum(AdvEngineID), count(*) from hits where SearchPhrase = 'alpha';");
        EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"SUM(AdvEngineID)", "COUNT(*)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"3", "2"}));
    }
}

TEST(executor, rejects_unknown_aggregate_functions) {
    const TempFile schema_file("executor_schema");
    const TempFile data_file("executor_data");
    const TempFile columnar_file("executor_columnar");

    WriteRows(schema_file.Path(), {
                                      {"Value", "int64"},
                                  });
    WriteRows(data_file.Path(), {
                                    {"1"},
                                });
    ConvertCsvToColumnar(schema_file.Path(), data_file.Path(), columnar_file.Path(), 1);

    Executor executor;
    executor.RegisterTable("hits", columnar_file.Path());

    const ExecuteExpected result = executor.Execute("SELECT median(Value) FROM hits;");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().GetCode(), Error::Code::Unsupported);
}

TEST(executor, supports_group_by_computed_numeric_and_timestamp_keys) {
    const TempFile schema_file("executor_computed_schema");
    const TempFile data_file("executor_computed_data");
    const TempFile columnar_file("executor_computed_columnar");

    WriteRows(schema_file.Path(), {
                                      {"ClientIP", "int64"},
                                      {"EventTime", "timestamp"},
                                  });
    WriteRows(data_file.Path(), {
                                    {"10", "2024-01-01 12:03:11"},
                                    {"11", "2024-01-01 12:03:49"},
                                    {"10", "2024-01-01 12:04:00"},
                                });

    ConvertCsvToColumnar(schema_file.Path(), data_file.Path(), columnar_file.Path(), 2);

    Executor executor;
    executor.RegisterTable("hits", columnar_file.Path());

    {
        auto result = executor.Execute(
            "SELECT ClientIP - 1, COUNT(*) AS c FROM hits GROUP BY ClientIP - 1 ORDER BY c DESC, ClientIP - 1 ASC;");
        ASSERT_TRUE(result.has_value()) << result.error().what();
        EXPECT_EQ(BatchRows(result.value()), (std::vector<std::vector<std::string>>{
                                                 {"9", "2"},
                                                 {"10", "1"},
                                             }));
    }

    {
        auto result = executor.Execute(
            "SELECT extract(minute FROM EventTime) AS m, COUNT(*) AS c FROM hits GROUP BY m ORDER BY m ASC;");
        ASSERT_TRUE(result.has_value()) << result.error().what();
        EXPECT_EQ(BatchRows(result.value()), (std::vector<std::vector<std::string>>{
                                                 {"3", "2"},
                                                 {"4", "1"},
                                             }));
    }

    {
        auto result = executor.Execute(
            "SELECT DATE_TRUNC('minute', EventTime) AS m, COUNT(*) AS c FROM hits GROUP BY m ORDER BY m ASC;");
        ASSERT_TRUE(result.has_value()) << result.error().what();
        EXPECT_EQ(BatchRows(result.value()), (std::vector<std::vector<std::string>>{
                                                 {"2024-01-01 12:03:00", "2"},
                                                 {"2024-01-01 12:04:00", "1"},
                                             }));
    }
}

TEST(executor, prunes_row_groups_for_typed_where_filters_before_reading_chunks) {
    const TempFile schema_file("executor_prune_schema");
    const TempFile data_file("executor_prune_data");
    const TempFile columnar_file("executor_prune_columnar");

    WriteRows(schema_file.Path(), {
                                      {"EventDate", "date"},
                                      {"Value", "int64"},
                                  });
    WriteRows(data_file.Path(), {
                                    {"2024-01-01", "1"},
                                    {"2024-01-01", "2"},
                                    {"2024-01-09", "3"},
                                    {"2024-01-09", "4"},
                                });

    ConvertCsvToColumnar(schema_file.Path(), data_file.Path(), columnar_file.Path(), 2);

    ColumnarBatchReader reader(columnar_file.Path());
    ColumnarMetadata metadata = reader.GetMetadata();
    ASSERT_EQ(metadata.row_groups.size(), 2u);
    ASSERT_EQ(metadata.row_groups[1].columns.size(), 2u);

    const auto file_info = GetFileMetadata(columnar_file.Path());
    ASSERT_TRUE(file_info.has_value());

    metadata.row_groups[1].columns[0].offset = file_info->size + 1024;

    std::ostringstream metadata_stream;
    WriteMetadata(metadata_stream, metadata);
    const std::string metadata_blob = metadata_stream.str();

    std::vector<uint8_t> bytes = ReadFileBytes(columnar_file.Path());
    ASSERT_GE(bytes.size(), sizeof(uint64_t) + 4u);

    const size_t footer_offset = bytes.size() - sizeof(uint64_t) - 4u;
    uint64_t metadata_size = 0;
    std::memcpy(&metadata_size, bytes.data() + footer_offset, sizeof(metadata_size));

    const size_t metadata_offset = footer_offset - metadata_size;
    bytes.erase(bytes.begin() + static_cast<std::ptrdiff_t>(metadata_offset),
                bytes.begin() + static_cast<std::ptrdiff_t>(footer_offset));
    bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(metadata_offset), metadata_blob.begin(),
                 metadata_blob.end());

    const uint64_t patched_metadata_size = metadata_blob.size();
    std::memcpy(bytes.data() + metadata_offset + metadata_blob.size(), &patched_metadata_size,
                sizeof(patched_metadata_size));

    WriteFileBytes(columnar_file.Path(), bytes);

    Executor executor;
    executor.RegisterTable("hits", columnar_file.Path());

    auto result = executor.Execute("SELECT COUNT(*) FROM hits WHERE EventDate = '2024-01-01';");
    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_EQ(SingleRowValues(result.value()), std::vector<std::string>{"2"});
}
