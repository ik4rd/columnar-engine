/* (What is this? — Парсеры) */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "schema.h"

bool ParseBoolean(const std::string& value);
int16_t ParseInt16(const std::string& value);
int32_t ParseInt32(const std::string& value);
int64_t ParseInt64(const std::string& value);
#if !defined(__SIZEOF_INT128__)
#error "This project requires compiler support for 128-bit integers"
#endif
__int128_t ParseInt128(const std::string& value);
int32_t ParseDate(const std::string& value);
int64_t ParseTimestamp(const std::string& value);
char ParseCharacter(const std::string& value);

std::string BooleanToString(bool value);
std::string Int128ToString(__int128_t value);
std::string DateToString(int32_t value);
std::string TimestampToString(int64_t value);

ColumnType ParseColumnType(std::string_view input);
std::string ColumnTypeToString(ColumnType type);
