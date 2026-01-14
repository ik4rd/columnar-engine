#pragma once

#include <string>
#include <string_view>

std::string ToLower(std::string_view input);

bool NeedsQuotes(std::string_view value);