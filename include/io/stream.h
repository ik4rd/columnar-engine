/* (What is this? — Обертки над потоками, чтение и запись) */

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

#include "error.h"

void ReadBytes(std::istream& in, char* dst, size_t size);
void WriteBytes(std::ostream& out, std::string_view bytes);

template <std::integral T>
T ReadStream(std::istream& in) {
    std::array<std::byte, sizeof(T)> bytes{};
    ReadBytes(in, reinterpret_cast<char*>(bytes.data()), bytes.size());

    if constexpr (std::endian::native == std::endian::big) {
        std::reverse(bytes.begin(), bytes.end());
    }

    T value = 0;
    std::memcpy(&value, bytes.data(), sizeof(value));
    return value;
}

template <std::integral T>
void WriteStream(std::ostream& out, T value) {
    std::array<std::byte, sizeof(T)> bytes{};
    std::memcpy(bytes.data(), &value, sizeof(value));

    if constexpr (std::endian::native == std::endian::big) {
        std::reverse(bytes.begin(), bytes.end());
    }

    WriteBytes(out, {reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}
