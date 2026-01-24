#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

struct Schema;

std::string ToLower(std::string_view input);

bool NeedsQuotes(std::string_view value);

bool SchemasEqual(const Schema& left, const Schema& right);

uint64_t AddBytesChecked(uint64_t current, uint64_t add);

uint64_t EstimateRowBytes(const Schema& schema, const std::vector<std::string>& row);

void CheckIndex(const std::string& module, size_t row, size_t size);
