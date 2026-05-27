#include "common/parsing.h"

#include <charconv>
#include <chrono>
#include <concepts>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>

#include "common/ascii.h"
#include "common/error.h"

constexpr size_t DateTextLength = 10;
constexpr size_t DateYearOffset = 0;
constexpr size_t DateYearLength = 4;
constexpr size_t DateMonthOffset = 5;
constexpr size_t DateMonthLength = 2;
constexpr size_t DateDayOffset = 8;
constexpr size_t DateDayLength = 2;
constexpr size_t DateFirstSeparatorIndex = 4;
constexpr size_t DateSecondSeparatorIndex = 7;

constexpr size_t TimestampMinLength = 19;
constexpr size_t TimestampDateLength = 10;
constexpr size_t TimestampSeparatorIndex = 10;
constexpr size_t TimestampHourOffset = 11;
constexpr size_t TimestampMinuteOffset = 14;
constexpr size_t TimestampSecondOffset = 17;
constexpr size_t TimestampComponentLength = 2;
constexpr size_t TimestampHourMinuteSeparatorIndex = 13;
constexpr size_t TimestampMinuteSecondSeparatorIndex = 16;
constexpr size_t TimestampFractionSeparatorIndex = 19;
constexpr size_t TimestampFractionDigitsOffset = 20;
constexpr size_t TimestampMaxFractionDigits = 6;

constexpr unsigned MaxHour = 23;
constexpr unsigned MaxMinute = 59;
constexpr unsigned MaxSecond = 59;

constexpr char DateSeparator = '-';
constexpr char TimestampDateTimeSeparator = ' ';
constexpr char TimestampAlternativeDateTimeSeparator = 'T';
constexpr char TimeSeparator = ':';
constexpr char FractionSeparator = '.';

template <std::integral T>
bool TryParseInteger(const std::string_view value, T& result) {
    if (value.empty()) {
        return false;
    }

    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto [ptr, ec] = std::from_chars(begin, end, result);

    return ec == std::errc() && ptr == end;
}

template <std::integral T>
T ParseInteger(const std::string_view value, const std::string_view type_name) {
    if (value.empty()) {
        throw Error::MalformedData("common", "empty value for " + std::string(type_name));
    }

    T result = 0;
    if (!TryParseInteger(value, result)) {
        throw Error::MalformedData("common", "invalid " + std::string(type_name) + " value");
    }

    return result;
}

static bool TryParseUnsignedPart(const std::string_view input, const size_t offset, const size_t length,
                                 unsigned& value) {
    const char* begin = input.data() + offset;
    const char* end = begin + length;
    const auto [ptr, ec] = std::from_chars(begin, end, value);
    return ec == std::errc() && ptr == end;
}

static bool TryParseDatePoint(const std::string_view value, std::chrono::sys_days& result) {
    if (value.size() != DateTextLength || value[DateFirstSeparatorIndex] != DateSeparator ||
        value[DateSecondSeparatorIndex] != DateSeparator) {
        return false;
    }

    unsigned year = 0;
    unsigned month = 0;
    unsigned day = 0;

    if (!TryParseUnsignedPart(value, DateYearOffset, DateYearLength, year) ||
        !TryParseUnsignedPart(value, DateMonthOffset, DateMonthLength, month) ||
        !TryParseUnsignedPart(value, DateDayOffset, DateDayLength, day)) {
        return false;
    }

    const std::chrono::year_month_day ymd{
        std::chrono::year{static_cast<int>(year)},
        std::chrono::month{month},
        std::chrono::day{day},
    };

    if (!ymd.ok()) {
        return false;
    }

    result = std::chrono::sys_days{ymd};

    return true;
}

static std::chrono::sys_days ParseDatePoint(const std::string_view value, const std::string_view type_name) {
    std::chrono::sys_days result{};
    if (!TryParseDatePoint(value, result)) {
        throw Error::MalformedData("common", "invalid " + std::string(type_name) + " value");
    }
    return result;
}

static std::string FormatDateParts(const std::chrono::year_month_day ymd) {
    std::ostringstream out;
    out << std::setfill('0') << std::setw(4) << static_cast<int>(ymd.year()) << '-' << std::setw(2)
        << static_cast<unsigned>(ymd.month()) << '-' << std::setw(2) << static_cast<unsigned>(ymd.day());
    return out.str();
}

std::optional<bool> TryParseBoolean(const std::string_view value) {
    const std::string lowered = ToLowerAscii(value);
    if (lowered == "true" || lowered == "1") {
        return true;
    }
    if (lowered == "false" || lowered == "0") {
        return false;
    }
    return std::nullopt;
}

bool ParseBoolean(const std::string_view value) {
    const std::optional<bool> result = TryParseBoolean(value);
    if (!result.has_value()) {
        throw Error::MalformedData("common", "invalid bool value");
    }
    return *result;
}

std::optional<int16_t> TryParseInt16(const std::string_view value) {
    int16_t result = 0;
    if (!TryParseInteger(value, result)) {
        return std::nullopt;
    }
    return result;
}

int16_t ParseInt16(const std::string_view value) { return ParseInteger<int16_t>(value, "int16"); }

std::optional<int32_t> TryParseInt32(const std::string_view value) {
    int32_t result = 0;
    if (!TryParseInteger(value, result)) {
        return std::nullopt;
    }
    return result;
}

int32_t ParseInt32(const std::string_view value) { return ParseInteger<int32_t>(value, "int32"); }

std::optional<int64_t> TryParseInt64(const std::string_view value) {
    int64_t result = 0;
    if (!TryParseInteger(value, result)) {
        return std::nullopt;
    }
    return result;
}

int64_t ParseInt64(const std::string_view value) { return ParseInteger<int64_t>(value, "int64"); }

std::optional<Int128> TryParseInt128(const std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }

    size_t pos = 0;
    bool negative = false;

    if (value[pos] == '+' || value[pos] == '-') {
        negative = value[pos] == '-';
        ++pos;
    }

    if (pos == value.size()) {
        return std::nullopt;
    }

    const UInt128 positive_limit = ~UInt128{0} >> 1;
    const UInt128 negative_limit = positive_limit + 1;
    const UInt128 limit = negative ? negative_limit : positive_limit;

    UInt128 magnitude = 0;

    for (; pos < value.size(); ++pos) {
        const char ch = value[pos];
        if (ch < '0' || ch > '9') {
            return std::nullopt;
        }

        const auto digit = static_cast<UInt128>(ch - '0');
        if (magnitude > (limit - digit) / 10) {
            return std::nullopt;
        }

        magnitude = magnitude * 10 + digit;
    }

    if (!negative) {
        return static_cast<Int128>(magnitude);
    }

    if (magnitude == negative_limit) {
        return -static_cast<Int128>(positive_limit) - 1;
    }

    return -static_cast<Int128>(magnitude);
}

Int128 ParseInt128(const std::string_view value) {
    if (value.empty()) {
        throw Error::MalformedData("common", "empty value for int128");
    }

    const std::optional<Int128> result = TryParseInt128(value);
    if (!result.has_value()) {
        throw Error::MalformedData("common", "invalid int128 value");
    }

    return *result;
}

std::optional<int32_t> TryParseDate(const std::string_view value) {
    std::chrono::sys_days day_point{};
    if (!TryParseDatePoint(value, day_point)) {
        return std::nullopt;
    }

    const auto days_since_epoch = day_point.time_since_epoch().count();

    if (days_since_epoch < std::numeric_limits<int32_t>::min() ||
        days_since_epoch > std::numeric_limits<int32_t>::max()) {
        return std::nullopt;
    }

    return days_since_epoch;
}

int32_t ParseDate(const std::string_view value) {
    const auto day_point = ParseDatePoint(value, "date");
    const auto days_since_epoch = day_point.time_since_epoch().count();

    if (days_since_epoch < std::numeric_limits<int32_t>::min() ||
        days_since_epoch > std::numeric_limits<int32_t>::max()) {
        throw Error::Overflow("common", "date value exceeds supported range");
    }

    return days_since_epoch;
}

std::optional<int64_t> TryParseTimestamp(const std::string_view value) {
    if (value.size() < TimestampMinLength) {
        return std::nullopt;
    }

    std::chrono::sys_days day_point{};
    if (!TryParseDatePoint(value.substr(0, TimestampDateLength), day_point)) {
        return std::nullopt;
    }

    if (value[TimestampSeparatorIndex] != TimestampDateTimeSeparator &&
        value[TimestampSeparatorIndex] != TimestampAlternativeDateTimeSeparator) {
        return std::nullopt;
    }

    if (value[TimestampHourMinuteSeparatorIndex] != TimeSeparator ||
        value[TimestampMinuteSecondSeparatorIndex] != TimeSeparator) {
        return std::nullopt;
    }

    unsigned hour = 0;
    unsigned minute = 0;
    unsigned second = 0;

    if (!TryParseUnsignedPart(value, TimestampHourOffset, TimestampComponentLength, hour) ||
        !TryParseUnsignedPart(value, TimestampMinuteOffset, TimestampComponentLength, minute) ||
        !TryParseUnsignedPart(value, TimestampSecondOffset, TimestampComponentLength, second)) {
        return std::nullopt;
    }

    if (hour > MaxHour || minute > MaxMinute || second > MaxSecond) {
        return std::nullopt;
    }

    int64_t microseconds = 0;

    if (value.size() > TimestampMinLength) {
        if (value[TimestampFractionSeparatorIndex] != FractionSeparator) {
            return std::nullopt;
        }

        const size_t digits = value.size() - TimestampFractionDigitsOffset;
        if (digits == 0 || digits > TimestampMaxFractionDigits) {
            return std::nullopt;
        }

        unsigned fractional = 0;
        if (!TryParseUnsignedPart(value, TimestampFractionDigitsOffset, digits, fractional)) {
            return std::nullopt;
        }

        microseconds = static_cast<int64_t>(fractional);
        for (size_t i = digits; i < TimestampMaxFractionDigits; ++i) {
            microseconds *= 10;
        }
    }

    const auto timestamp = day_point + std::chrono::hours{hour} + std::chrono::minutes{minute} +
                           std::chrono::seconds{second} + std::chrono::microseconds{microseconds};

    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}

int64_t ParseTimestamp(const std::string_view value) {
    const std::optional<int64_t> result = TryParseTimestamp(value);
    if (!result.has_value()) {
        throw Error::MalformedData("common", "invalid timestamp value");
    }
    return *result;
}

std::optional<char> TryParseCharacter(const std::string_view value) {
    if (value.size() != 1) {
        return std::nullopt;
    }
    return value[0];
}

char ParseCharacter(const std::string_view value) {
    const std::optional<char> result = TryParseCharacter(value);
    if (!result.has_value()) {
        throw Error::MalformedData("common", "invalid char value");
    }
    return *result;
}

std::string BooleanToString(const bool value) { return value ? "true" : "false"; }

std::string Int128ToString(const Int128 value) {
    if (value == 0) {
        return "0";
    }

    const bool negative = value < 0;
    UInt128 magnitude = 0;

    if (negative) {
        magnitude = static_cast<UInt128>(-(value + 1));
        ++magnitude;
    } else {
        magnitude = static_cast<UInt128>(value);
    }

    std::string out;

    while (magnitude > 0) {
        out.push_back(static_cast<char>('0' + magnitude % 10));
        magnitude /= 10;
    }

    if (negative) {
        out.push_back('-');
    }

    std::reverse(out.begin(), out.end());

    return out;
}

std::string DateToString(const int32_t value) {
    const auto day_point = std::chrono::sys_days{} + std::chrono::days{value};
    return FormatDateParts(std::chrono::year_month_day{day_point});
}

std::string TimestampToString(const int64_t value) {
    const std::chrono::sys_time<std::chrono::microseconds> timestamp{std::chrono::microseconds{value}};
    const auto day_point = std::chrono::floor<std::chrono::days>(timestamp);
    const std::chrono::hh_mm_ss tod{timestamp - day_point};

    std::ostringstream out;
    out << FormatDateParts(std::chrono::year_month_day{day_point}) << TimestampDateTimeSeparator
        << std::setfill('0') << std::setw(2) << tod.hours().count() << TimeSeparator << std::setw(2)
        << tod.minutes().count() << TimeSeparator << std::setw(2) << tod.seconds().count();

    const int64_t fractional = tod.subseconds().count();

    if (fractional != 0) {
        std::string frac = std::to_string(fractional);
        frac.insert(frac.begin(), TimestampMaxFractionDigits - static_cast<std::ptrdiff_t>(frac.size()), '0');
        while (!frac.empty() && frac.back() == '0') {
            frac.pop_back();
        }
        out << FractionSeparator << frac;
    }

    return out.str();
}

ColumnType ParseColumnType(const std::string_view input) {
    const std::string lowered = ToLowerAscii(input);

    if (lowered == "bool") {
        return ColumnType::Boolean;
    }

    if (lowered == "int16") {
        return ColumnType::Int16;
    }

    if (lowered == "int32") {
        return ColumnType::Int32;
    }

    if (lowered == "int64") {
        return ColumnType::Int64;
    }

    if (lowered == "int128") {
        return ColumnType::Int128;
    }

    if (lowered == "string") {
        return ColumnType::String;
    }

    if (lowered == "date") {
        return ColumnType::Date;
    }

    if (lowered == "timestamp") {
        return ColumnType::Timestamp;
    }

    if (lowered == "char") {
        return ColumnType::Character;
    }

    throw Error::Unsupported("common", "unknown column type");
}

std::string ColumnTypeToString(const ColumnType type) {
    switch (type) {
        case ColumnType::Boolean:
            return "bool";
        case ColumnType::Int16:
            return "int16";
        case ColumnType::Int32:
            return "int32";
        case ColumnType::Int64:
            return "int64";
        case ColumnType::Int128:
            return "int128";
        case ColumnType::String:
            return "string";
        case ColumnType::Date:
            return "date";
        case ColumnType::Timestamp:
            return "timestamp";
        case ColumnType::Character:
            return "char";
    }

    throw Error::Unsupported("common", "unknown column type");
}
