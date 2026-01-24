#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <istream>
#include <ostream>
#include <string_view>
#include <type_traits>

#include "error.h"

void ReadBytes(std::istream& in, char* dst, size_t size);
void WriteBytes(std::ostream& out, std::string_view bytes);

template <typename T>
T ReadLittleEndian(std::istream& in) {
    static_assert(std::is_integral_v<T>, "ReadLittleEndian only supports integral types");

    std::array<uint8_t, sizeof(T)> bytes;
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (in.gcount() != static_cast<std::streamsize>(bytes.size()) || !in) {
        throw error::MakeError("stream", "failed to read binary data");
    }

    using Unsigned = std::make_unsigned_t<T>;
    Unsigned value = 0;
    for (size_t i = 0; i < bytes.size(); ++i) {
        value |= static_cast<Unsigned>(bytes[i]) << (8 * i);
    }

    return static_cast<T>(value);
}

template <typename T>
void WriteLittleEndian(std::ostream& out, T value) {
    static_assert(std::is_integral_v<T>, "WriteLittleEndian only supports integral types");

    using Unsigned = std::make_unsigned_t<T>;
    std::array<uint8_t, sizeof(T)> bytes;
    for (size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<uint8_t>(static_cast<Unsigned>(value) >> (8 * i) & 0xFF);
    }

    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!out) {
        throw error::MakeError("stream", "failed to write binary data");
    }
}
