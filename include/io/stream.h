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
#include <type_traits>

#include "common/error.h"

void ReadBytes(std::istream& in, char* dst, size_t size);
void WriteBytes(std::ostream& out, std::string_view bytes);

template <class T>
    requires std::is_trivially_copyable_v<T>
T ReadStream(std::istream& in) {
    T value{};
    ReadBytes(in, reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

template <class T>
    requires std::is_trivially_copyable_v<T>
void WriteStream(std::ostream& out, T value) {
    WriteBytes(out, {reinterpret_cast<const char*>(&value), sizeof(value)});
}
