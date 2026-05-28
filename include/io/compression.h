#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

enum class Compression : uint8_t {
    None = 0,
    Lz4 = 1,
};

const char* CompressionName(Compression compression);
Compression CompressionFromName(std::string_view name);

std::vector<uint8_t> Compress(std::span<const uint8_t> input, Compression compression);
std::vector<uint8_t> Decompress(std::span<const uint8_t> input, Compression compression, uint64_t uncompressed_size);
