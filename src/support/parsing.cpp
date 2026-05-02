#include "parsing.h"

#include <charconv>
#include <chrono>
#include <iomanip>
#include <limits>
#include <sstream>

#include "ascii.h"
#include "error.h"

template <std::integral T>
T ParseInteger(const std::string& value, const std::string_view type_name) {
    if (value.empty()) {
        throw Error::InvalidData("parsing", "empty value for " + std::string(type_name));
    }

    T result = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();

    if (const auto [ptr, ec] = std::from_chars(begin, end, result); ec != std::errc() || ptr != end) {
        throw Error::InvalidData("parsing", "invalid " + std::string(type_name) + " value");
    }

    return result;
}

static unsigned ParseUnsignedPart(const std::string_view input, const size_t offset, const size_t length,
                                  const std::string_view type_name) {
    unsigned value = 0;
    const char* begin = input.data() + offset;
    const char* end = begin + length;

    if (const auto [ptr, ec] = std::from_chars(begin, end, value); ec != std::errc() || ptr != end) {
        throw Error::InvalidData("parsing", "invalid " + std::string(type_name) + " value");
    }

    return value;
}

static std::chrono::sys_days ParseDatePoint(const std::string_view value, const std::string_view type_name) {
    if (value.size() != 10 || value[4] != '-' || value[7] != '-') {
        throw Error::InvalidData("parsing", "invalid " + std::string(type_name) + " value");
    }

    const int year = static_cast<int>(ParseUnsignedPart(value, 0, 4, type_name));
    const unsigned month = ParseUnsignedPart(value, 5, 2, type_name);
    const unsigned day = ParseUnsignedPart(value, 8, 2, type_name);

    const std::chrono::year_month_day ymd{
        std::chrono::year{year},
        std::chrono::month{month},
        std::chrono::day{day},
    };

    if (!ymd.ok()) {
        throw Error::InvalidData("parsing", "invalid " + std::string(type_name) + " value");
    }

    return std::chrono::sys_days{ymd};
}

static std::string FormatDateParts(const std::chrono::year_month_day ymd) {
    std::ostringstream out;
    out << std::setfill('0') << std::setw(4) << static_cast<int>(ymd.year()) << '-' << std::setw(2)
        << static_cast<unsigned>(ymd.month()) << '-' << std::setw(2) << static_cast<unsigned>(ymd.day());
    return out.str();
}

bool ParseBoolean(const std::string& value) {
    const std::string lowered = ToLowerAscii(value);
    if (lowered == "true" || lowered == "1") {
        return true;
    }
    if (lowered == "false" || lowered == "0") {
        return false;
    }
    throw Error::InvalidData("parsing", "invalid bool value");
}

int16_t ParseInt16(const std::string& value) { return ParseInteger<int16_t>(value, "int16"); }

int32_t ParseInt32(const std::string& value) { return ParseInteger<int32_t>(value, "int32"); }

int64_t ParseInt64(const std::string& value) { return ParseInteger<int64_t>(value, "int64"); }

Int128 ParseInt128(const std::string& value) {
    if (value.empty()) {
        throw Error::InvalidData("parsing", "empty value for int128");
    }

    size_t pos = 0;
    bool negative = false;

    if (value[pos] == '+' || value[pos] == '-') {
        negative = value[pos] == '-';
        ++pos;
    }

    if (pos == value.size()) {
        throw Error::InvalidData("parsing", "invalid int128 value");
    }

    const UInt128 positive_limit = ~UInt128{0} >> 1;
    const UInt128 negative_limit = positive_limit + 1;
    const UInt128 limit = negative ? negative_limit : positive_limit;

    UInt128 magnitude = 0;

    for (; pos < value.size(); ++pos) {
        const char ch = value[pos];
        if (ch < '0' || ch > '9') {
            throw Error::InvalidData("parsing", "invalid int128 value");
        }

        const auto digit = static_cast<UInt128>(ch - '0');
        if (magnitude > (limit - digit) / 10) {
            throw Error::InvalidData("parsing", "invalid int128 value");
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

int32_t ParseDate(const std::string& value) {
    const auto day_point = ParseDatePoint(value, "date");
    const auto days_since_epoch = day_point.time_since_epoch().count();

    if (days_since_epoch < std::numeric_limits<int32_t>::min() ||
        days_since_epoch > std::numeric_limits<int32_t>::max()) {
        throw Error::Overflow("parsing", "date value exceeds supported range");
    }

    return days_since_epoch;
}

int64_t ParseTimestamp(const std::string& value) {
    if (value.size() < 19) {
        throw Error::InvalidData("parsing", "invalid timestamp value");
    }

    const auto day_point = ParseDatePoint(std::string_view(value).substr(0, 10), "timestamp");

    if (value[10] != ' ' && value[10] != 'T') {
        throw Error::InvalidData("parsing", "invalid timestamp value");
    }
    if (value[13] != ':' || value[16] != ':') {
        throw Error::InvalidData("parsing", "invalid timestamp value");
    }

    const unsigned hour = ParseUnsignedPart(value, 11, 2, "timestamp");
    const unsigned minute = ParseUnsignedPart(value, 14, 2, "timestamp");
    const unsigned second = ParseUnsignedPart(value, 17, 2, "timestamp");

    if (hour > 23 || minute > 59 || second > 59) {
        throw Error::InvalidData("parsing", "invalid timestamp value");
    }

    int64_t microseconds = 0;

    if (value.size() > 19) {
        if (value[19] != '.') {
            throw Error::InvalidData("parsing", "invalid timestamp value");
        }

        const size_t digits = value.size() - 20;
        if (digits == 0 || digits > 6) {
            throw Error::InvalidData("parsing", "invalid timestamp value");
        }

        const auto fractional = ParseUnsignedPart(value, 20, digits, "timestamp");
        microseconds = static_cast<int64_t>(fractional);
        for (size_t i = digits; i < 6; ++i) {
            microseconds *= 10;
        }
    }

    const auto timestamp = day_point + std::chrono::hours{hour} + std::chrono::minutes{minute} +
                           std::chrono::seconds{second} + std::chrono::microseconds{microseconds};

    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}

char ParseCharacter(const std::string& value) {
    if (value.size() != 1) {
        throw Error::InvalidData("parsing", "invalid char value");
    }
    return value[0];
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
    const std::chrono::hh_mm_ss<std::chrono::microseconds> tod{timestamp - day_point};

    std::ostringstream out;
    out << FormatDateParts(std::chrono::year_month_day{day_point}) << ' ' << std::setfill('0') << std::setw(2)
        << tod.hours().count() << ':' << std::setw(2) << tod.minutes().count() << ':' << std::setw(2)
        << tod.seconds().count();

    const int64_t fractional = tod.subseconds().count();

    if (fractional != 0) {
        std::string frac = std::to_string(fractional);
        frac.insert(frac.begin(), 6 - static_cast<std::ptrdiff_t>(frac.size()), '0');
        while (!frac.empty() && frac.back() == '0') {
            frac.pop_back();
        }
        out << '.' << frac;
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
    throw Error::Unsupported("parsing", "unknown column type");
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
    throw Error::Unsupported("parsing", "unknown column type");
}
