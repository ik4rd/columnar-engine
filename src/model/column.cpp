#include "model/column.h"

#include <memory>

#include "model/column_boolean.h"
#include "model/column_character.h"
#include "model/column_date.h"
#include "model/column_int128.h"
#include "model/column_int16.h"
#include "model/column_int32.h"
#include "model/column_int64.h"
#include "model/column_string.h"
#include "model/column_timestamp.h"
#include "common/error.h"

std::unique_ptr<MutableColumn> CreateColumn(const ColumnType type) {
    switch (type) {
        case ColumnType::Int64:
            return std::make_unique<Int64Column>();
        case ColumnType::String:
            return std::make_unique<StringColumn>();
        case ColumnType::Boolean:
            return std::make_unique<BooleanColumn>();
        case ColumnType::Int16:
            return std::make_unique<Int16Column>();
        case ColumnType::Int32:
            return std::make_unique<Int32Column>();
        case ColumnType::Int128:
            return std::make_unique<Int128Column>();
        case ColumnType::Date:
            return std::make_unique<DateColumn>();
        case ColumnType::Timestamp:
            return std::make_unique<TimestampColumn>();
        case ColumnType::Character:
            return std::make_unique<CharacterColumn>();
    }

    throw Error::Unsupported("model", "unsupported column type");
}

Column::Column(const ColumnType type) : type_(type) {}

void Column::CheckRowIndex(const char* module, const size_t row, const size_t size) {
    if (row >= size) {
        throw Error::OutOfRange(module, "row index out of range");
    }
}
