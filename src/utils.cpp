#include "utils.h"

#include "error.h"

std::string ToLower(const std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (const unsigned char ch : input) {
        output.push_back(static_cast<char>(std::tolower(ch)));
    }
    return output;
}

bool NeedsQuotes(const std::string_view value) { return value.find_first_of(",\"\n\r") != std::string_view::npos; }
