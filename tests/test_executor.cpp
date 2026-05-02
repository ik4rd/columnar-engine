#include <string>
#include <vector>

#include "csv.h"
#include "csv_columnar.h"
#include "executor.h"
#include "executor_test_utils.h"
#include "gtest/gtest.h"
#include "temp_file.h"

static Batch BuildHitsTable(const std::string_view query) {
    const TempFile schema_file("executor_schema");
    const TempFile data_file("executor_data");
    const TempFile columnar_file("executor_columnar");

    WriteRows(schema_file.Path(), {
                                      {"AdvEngineID", "int64"},
                                      {"ResolutionWidth", "int32"},
                                      {"UserID", "int64"},
                                      {"SearchPhrase", "string"},
                                      {"EventDate", "date"},
                                  });

    WriteRows(data_file.Path(), {
                                    {"0", "100", "1", "alpha", "2024-01-05"},
                                    {"7", "200", "2", "beta", "2024-01-03"},
                                    {"3", "301", "2", "alpha", "2024-01-07"},
                                    {"7", "500", "5", "gamma", "2024-01-01"},
                                    {"1", "400", "11", "delta", "2024-01-09"},
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

TEST(executor, executes_basic_aggregate_queries) {
    {
        const Batch batch = BuildHitsTable("SELECT COUNT(*) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"COUNT(*)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"5"}));
    }

    {
        const Batch batch = BuildHitsTable("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;");
        EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"COUNT(*)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"4"}));
    }

    {
        const Batch batch = BuildHitsTable("SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch),
                  (std::vector<std::string>{"SUM(AdvEngineID)", "COUNT(*)", "AVG(ResolutionWidth)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"18", "5", "300.2"}));
    }

    {
        const Batch batch = BuildHitsTable("SELECT AVG(UserID) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"AVG(UserID)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"4.2"}));
    }

    {
        const Batch batch = BuildHitsTable("SELECT COUNT(DISTINCT UserID) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"COUNT(DISTINCT UserID)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"4"}));
    }

    {
        const Batch batch = BuildHitsTable("SELECT COUNT(DISTINCT SearchPhrase) FROM hits;");
        EXPECT_EQ(BatchColumnNames(batch), (std::vector<std::string>{"COUNT(DISTINCT SearchPhrase)"}));
        EXPECT_EQ(SingleRowValues(batch), (std::vector<std::string>{"4"}));
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
