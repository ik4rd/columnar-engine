#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "schema.h"

int64_t ParseInt64(const std::string& value);

ColumnType ParseColumnType(std::string_view input);
std::string ColumnTypeToString(ColumnType type);
