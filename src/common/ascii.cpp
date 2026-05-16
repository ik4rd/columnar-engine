#include "common/ascii.h"

#include <cctype>

std::string ToLowerAscii(const std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (const unsigned char ch : input) {
        output.push_back(static_cast<char>(std::tolower(ch)));
    }
    return output;
}

std::string ToUpperAscii(const std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (const unsigned char ch : input) {
        output.push_back(static_cast<char>(std::toupper(ch)));
    }
    return output;
}
