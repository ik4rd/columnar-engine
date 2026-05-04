#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <istream>
#include <ostream>
#include <string_view>

#include "support/error.h"

void ReadBytes(std::istream& in, char* dst, size_t size);
void WriteBytes(std::ostream& out, std::string_view bytes);

template <std::integral T>
T ReadStream(std::istream& in) {
    T value = 0;
    ReadBytes(in, reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

template <std::integral T>
void WriteStream(std::ostream& out, T value) {
    WriteBytes(out, {reinterpret_cast<const char*>(&value), sizeof(value)});
}
