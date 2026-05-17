#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "model/schema.h"
#include "common/int128.h"

bool ParseBoolean(const std::string& value);
int16_t ParseInt16(const std::string& value);
int32_t ParseInt32(const std::string& value);
int64_t ParseInt64(const std::string& value);
Int128 ParseInt128(const std::string& value);
int32_t ParseDate(const std::string& value);
int64_t ParseTimestamp(const std::string& value);
char ParseCharacter(const std::string& value);

std::optional<bool> TryParseBoolean(std::string_view value);
std::optional<int16_t> TryParseInt16(std::string_view value);
std::optional<int32_t> TryParseInt32(std::string_view value);
std::optional<int64_t> TryParseInt64(std::string_view value);
std::optional<Int128> TryParseInt128(std::string_view value);
std::optional<int32_t> TryParseDate(std::string_view value);
std::optional<int64_t> TryParseTimestamp(std::string_view value);
std::optional<char> TryParseCharacter(std::string_view value);

std::string BooleanToString(bool value);
std::string Int128ToString(Int128 value);
std::string DateToString(int32_t value);
std::string TimestampToString(int64_t value);

ColumnType ParseColumnType(std::string_view input);
std::string ColumnTypeToString(ColumnType type);
