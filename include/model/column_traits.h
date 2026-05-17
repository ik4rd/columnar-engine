#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "common/error.h"
#include "common/int128.h"
#include "common/parsing.h"
#include "model/schema.h"

template <ColumnType TypeValue>
struct ColumnValueTraits;

template <>
struct ColumnValueTraits<ColumnType::Boolean> {
    using Type = uint8_t;

    static Type Parse(const std::string& value) { return ParseBoolean(value) ? 1 : 0; }
    static std::string ToString(const Type value) { return BooleanToString(value != 0); }
};

template <>
struct ColumnValueTraits<ColumnType::Int16> {
    using Type = int16_t;

    static Type Parse(const std::string& value) { return ParseInt16(value); }
    static std::string ToString(const Type value) { return std::to_string(value); }
};

template <>
struct ColumnValueTraits<ColumnType::Int32> {
    using Type = int32_t;

    static Type Parse(const std::string& value) { return ParseInt32(value); }
    static std::string ToString(const Type value) { return std::to_string(value); }
};

template <>
struct ColumnValueTraits<ColumnType::Int64> {
    using Type = int64_t;

    static Type Parse(const std::string& value) { return ParseInt64(value); }
    static std::string ToString(const Type value) { return std::to_string(value); }
};

template <>
struct ColumnValueTraits<ColumnType::Int128> {
    using Type = Int128;

    static Type Parse(const std::string& value) { return ParseInt128(value); }
    static std::string ToString(const Type value) { return Int128ToString(value); }
};

template <>
struct ColumnValueTraits<ColumnType::Date> {
    using Type = int32_t;

    static Type Parse(const std::string& value) { return ParseDate(value); }
    static std::string ToString(const Type value) { return DateToString(value); }
};

template <>
struct ColumnValueTraits<ColumnType::Timestamp> {
    using Type = int64_t;

    static Type Parse(const std::string& value) { return ParseTimestamp(value); }
    static std::string ToString(const Type value) { return TimestampToString(value); }
};

template <>
struct ColumnValueTraits<ColumnType::Character> {
    using Type = char;

    static Type Parse(const std::string& value) { return ParseCharacter(value); }
    static std::string ToString(const Type value) { return std::string(1, value); }
};

template <>
struct ColumnValueTraits<ColumnType::String> {
    using Type = std::string;

    static Type Parse(const std::string& value) { return value; }
    static std::string ToString(const Type& value) { return value; }
};

template <class Visitor>
decltype(auto) VisitColumnType(const ColumnType type, Visitor&& visitor) {
    switch (type) {
        case ColumnType::Boolean:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Boolean>();
        case ColumnType::Int16:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Int16>();
        case ColumnType::Int32:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Int32>();
        case ColumnType::Int64:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Int64>();
        case ColumnType::Int128:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Int128>();
        case ColumnType::String:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::String>();
        case ColumnType::Date:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Date>();
        case ColumnType::Timestamp:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Timestamp>();
        case ColumnType::Character:
            return std::forward<Visitor>(visitor).template operator()<ColumnType::Character>();
    }

    throw Error::Unsupported("model", "unsupported column type");
}
