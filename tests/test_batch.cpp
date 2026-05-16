#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "io/columnar_batch.h"
#include "io/csv.h"
#include "io/csv_batch.h"
#include "common/error.h"
#include "testing/temp_file.h"

static_assert(std::is_copy_constructible_v<Batch>);
static_assert(std::is_copy_assignable_v<Batch>);
static_assert(std::is_move_constructible_v<Batch>);
static_assert(std::is_move_assignable_v<Batch>);

TEST(batch, usage_example_csv_to_columnar_roundtrip) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile csv_file("batch_usage_csv");
    const TempFile columnar_file("batch_usage_columnar");
    WriteRows(csv_file.Path(), {
                                   {"1", "alpha"},
                                   {"2", "beta"},
                               });

    CsvBatchReader csv_reader(csv_file.Path(), schema, {});
    ColumnarBatchWriter columnar_writer(columnar_file.Path(), schema);

    while (auto batch = csv_reader.ReadNext()) {
        columnar_writer.Write(*batch);
    }
    std::move(columnar_writer).Finalize();

    ColumnarBatchReader columnar_reader(columnar_file.Path());
    auto batch = columnar_reader.ReadNext();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->RowsCount(), 2u);
    EXPECT_EQ(batch->ColumnAt(0).ValueAsString(0), "1");
    EXPECT_EQ(batch->ColumnAt(1).ValueAsString(1), "beta");
    EXPECT_FALSE(columnar_reader.ReadNext().has_value());
}

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

TEST(batch, write_batch_csv_writes_single_batch) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    Batch batch(schema);
    batch.AppendValueFromString(0, "1");
    batch.AppendValueFromString(1, "alpha");
    batch.AppendValueFromString(0, "2");
    batch.AppendValueFromString(1, "be,ta");

    const TempFile data_out("batch_write_csv");
    WriteBatchCsv(data_out.Path(), batch);

    EXPECT_EQ(ReadRows(data_out.Path()), (std::vector<std::vector<std::string>>{
                                             {"1", "alpha"},
                                             {"2", "be,ta"},
                                         }));
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
    std::move(columnar_writer).Finalize();

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
    batch.AppendValueFromString(0, "1");
    batch.AppendValueFromString(0, "2");
    batch.AppendValueFromString(1, "alpha");

    EXPECT_THROW(batch.Validate(), Error);
}

TEST(batch, copy_is_deep) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    Batch original(schema);
    original.AppendValueFromString(0, "1");
    original.AppendValueFromString(1, "alpha");

    Batch copied = original;
    copied.AppendValueFromString(0, "2");
    copied.AppendValueFromString(1, "beta");

    EXPECT_EQ(original.RowsCount(), 1u);
    EXPECT_EQ(original.ColumnAt(0).ValueAsString(0), "1");
    EXPECT_EQ(original.ColumnAt(1).ValueAsString(0), "alpha");

    EXPECT_EQ(copied.RowsCount(), 2u);
    EXPECT_EQ(copied.ColumnAt(0).ValueAsString(1), "2");
    EXPECT_EQ(copied.ColumnAt(1).ValueAsString(1), "beta");
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

    EXPECT_THROW(reader.ReadNext(), Error);
}

TEST(batch, columnar_writer_rejects_after_finalize) {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::Int64},
        {"name", ColumnType::String},
    };

    const TempFile columnar_file("batch_finalize_columnar");
    ColumnarBatchWriter writer(columnar_file.Path(), schema);
    std::move(writer).Finalize();

    Batch batch(schema);
    EXPECT_THROW(writer.Write(batch), Error);
    EXPECT_THROW(std::move(writer).Finalize(), Error);
}
