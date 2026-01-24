#include "column.h"

#include <memory>

#include "column_int64.h"
#include "column_string.h"
#include "error.h"

std::unique_ptr<Column> CreateColumn(const ColumnType type) {
    switch (type) {
        case ColumnType::Int64:
            return std::make_unique<Int64Column>();
        case ColumnType::String:
            return std::make_unique<StringColumn>();
    }
    throw error::MakeError("column", "unsupported column type");
}

Column::Column(const ColumnType type) : type_(type) {}
