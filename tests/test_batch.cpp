#include <stdexcept>
#include <string>
#include <vector>

#include "batch_io.h"
#include "csv.h"
#include "gtest/gtest.h"
#include "temp_file.h"

namespace {
void AppendBatchRows(const Batch& batch, std::vector<std::vector<std::string>>& rows) {
    const size_t row_count = batch.RowsCount();
    const size_t column_count = batch.ColumnsCount();

    for (size_t row = 0; row < row_count; ++row) {
        std::vector<std::string> values;
        values.reserve(column_count);
        for (size_t col = 0; col < column_count; ++col) {
            values.push_back(batch.ColumnAt(col).ValueAsString(row));
        }
        rows.push_back(std::move(values));
    }
}
}  // namespace

TEST(batch, csv_reader_respects_max_rows) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile data_in("batch_csv");
    const std::vector<std::vector<std::string>> data_rows = {
        {"1", "alpha"},
        {"2", "be,ta"},
        {"3", "gamma"},
    };

    WriteRows(data_in.Path(), data_rows);

    BatchSizing sizing;
    sizing.max_rows = 2;
    CsvBatchReader reader(data_in.Path(), schema, sizing);

    auto first = reader.ReadNext();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->RowsCount(), 2u);
    EXPECT_EQ(first->ColumnAt(0).ValueAsString(0), "1");
    EXPECT_EQ(first->ColumnAt(1).ValueAsString(1), "be,ta");

    auto second = reader.ReadNext();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->RowsCount(), 1u);
    EXPECT_EQ(second->ColumnAt(0).ValueAsString(0), "3");
    EXPECT_EQ(second->ColumnAt(1).ValueAsString(0), "gamma");

    EXPECT_FALSE(reader.ReadNext().has_value());
}

TEST(batch, columnar_roundtrip) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile data_in("batch_roundtrip_csv");
    const TempFile columnar_file("batch_roundtrip_columnar");

    const std::vector<std::vector<std::string>> data_rows = {
        {"1", "alpha"},
        {"2", "be,ta"},
        {"3", "gamma"},
    };

    WriteRows(data_in.Path(), data_rows);

    BatchSizing sizing;
    sizing.max_rows = 2;
    CsvBatchReader csv_reader(data_in.Path(), schema, sizing);
    ColumnarBatchWriter columnar_writer(columnar_file.Path(), schema);

    while (auto batch = csv_reader.ReadNext()) {
        columnar_writer.Write(*batch);
    }
    columnar_writer.Finalize();

    ColumnarBatchReader columnar_reader(columnar_file.Path());
    EXPECT_EQ(columnar_reader.GetMetadata().row_groups.size(), 2u);

    std::vector<std::vector<std::string>> roundtrip_rows;
    while (auto batch = columnar_reader.ReadNext()) {
        AppendBatchRows(*batch, roundtrip_rows);
    }

    EXPECT_EQ(roundtrip_rows, data_rows);
}

TEST(batch, validate_detects_row_count_mismatch) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    Batch batch(schema);
    batch.ColumnAt(0).AppendFromString("1");
    batch.ColumnAt(0).AppendFromString("2");
    batch.ColumnAt(1).AppendFromString("alpha");

    EXPECT_THROW(batch.Validate(), std::runtime_error);
}

TEST(batch, csv_reader_respects_max_values) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile data_in("batch_csv_values");
    const std::vector<std::vector<std::string>> data_rows = {
        {"1", "alpha"},
        {"2", "beta"},
        {"3", "gamma"},
    };

    WriteRows(data_in.Path(), data_rows);

    BatchSizing sizing;
    sizing.max_values = 3;
    CsvBatchReader reader(data_in.Path(), schema, sizing);

    for (size_t i = 0; i < data_rows.size(); ++i) {
        auto batch = reader.ReadNext();
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(batch->RowsCount(), 1u);
        EXPECT_EQ(batch->ColumnAt(0).ValueAsString(0), data_rows[i][0]);
        EXPECT_EQ(batch->ColumnAt(1).ValueAsString(0), data_rows[i][1]);
    }

    EXPECT_FALSE(reader.ReadNext().has_value());
}

TEST(batch, csv_reader_respects_max_bytes) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile data_in("batch_csv_bytes");
    const std::vector<std::vector<std::string>> data_rows = {
        {"1", "aaaa"},
        {"2", "bbbb"},
        {"3", "cccc"},
    };

    WriteRows(data_in.Path(), data_rows);

    BatchSizing sizing;
    sizing.max_bytes = sizeof(int64_t) + 4u;
    CsvBatchReader reader(data_in.Path(), schema, sizing);

    for (size_t i = 0; i < data_rows.size(); ++i) {
        auto batch = reader.ReadNext();
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(batch->RowsCount(), 1u);
        EXPECT_EQ(batch->ColumnAt(0).ValueAsString(0), data_rows[i][0]);
        EXPECT_EQ(batch->ColumnAt(1).ValueAsString(0), data_rows[i][1]);
    }

    EXPECT_FALSE(reader.ReadNext().has_value());
}

TEST(batch, csv_reader_throws_on_column_mismatch) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile data_in("batch_csv_mismatch");
    WriteRows(data_in.Path(), {{"1"}});

    BatchSizing sizing;
    sizing.max_rows = 2;
    CsvBatchReader reader(data_in.Path(), schema, sizing);

    EXPECT_THROW(reader.ReadNext(), std::runtime_error);
}

TEST(batch, columnar_writer_rejects_after_finalize) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile columnar_file("batch_finalize_columnar");
    ColumnarBatchWriter writer(columnar_file.Path(), schema);
    writer.Finalize();

    Batch batch(schema);
    EXPECT_THROW(writer.Write(batch), std::runtime_error);
    EXPECT_THROW(writer.Finalize(), std::runtime_error);
}
