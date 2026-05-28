#include "io/compression.h"

#include <lz4.h>

#include <algorithm>
#include <limits>
#include <string>

#include "common/error.h"

const char* CompressionName(const Compression compression) {
    switch (compression) {
        case Compression::None:
            return "none";
        case Compression::Lz4:
            return "lz4";
    }

    throw Error::InvalidArgument("compression", "unknown compression codec");
}

Compression CompressionFromName(const std::string_view name) {
    if (name == "none") {
        return Compression::None;
    }
    if (name == "lz4") {
        return Compression::Lz4;
    }

    throw Error::InvalidArgument("compression", "unsupported compression codec: " + std::string(name));
}

std::vector<uint8_t> Compress(const std::span<const uint8_t> input, const Compression compression) {
    switch (compression) {
        case Compression::None:
            return std::vector<uint8_t>(input.begin(), input.end());
        case Compression::Lz4:
            break;
    }

    if (input.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw Error::Overflow("compression", "input too large for lz4");
    }

    const int upper_bound = LZ4_compressBound(static_cast<int>(input.size()));
    if (upper_bound <= 0) {
        throw Error::InvalidState("compression", "lz4 upper bound failed");
    }

    std::vector<uint8_t> output(static_cast<size_t>(upper_bound));
    const int compressed_size =
        LZ4_compress_default(reinterpret_cast<const char*>(input.data()), reinterpret_cast<char*>(output.data()),
                             static_cast<int>(input.size()), upper_bound);

    if (compressed_size <= 0) {
        throw Error::Io("compression", "lz4 compression failed");
    }

    output.resize(static_cast<size_t>(compressed_size));

    return output;
}

std::vector<uint8_t> Decompress(const std::span<const uint8_t> input, const Compression compression,
                                const uint64_t uncompressed_size) {
    switch (compression) {
        case Compression::None:
            if (input.size() != uncompressed_size) {
                throw Error::MalformedData("compression", "uncompressed chunk size mismatch");
            }
            return std::vector<uint8_t>(input.begin(), input.end());
        case Compression::Lz4:
            break;
    }

    if (input.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        throw Error::Overflow("compression", "compressed input too large for lz4");
    }
    if (uncompressed_size > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        throw Error::Overflow("compression", "uncompressed output too large for lz4");
    }

    std::vector<uint8_t> output(uncompressed_size);
    const int decompressed_size =
        LZ4_decompress_safe(reinterpret_cast<const char*>(input.data()), reinterpret_cast<char*>(output.data()),
                            static_cast<int>(input.size()), static_cast<int>(uncompressed_size));

    if (decompressed_size < 0) {
        throw Error::MalformedData("compression", "lz4 decompression failed");
    }

    if (static_cast<uint64_t>(decompressed_size) != uncompressed_size) {
        throw Error::MalformedData("compression", "lz4 decompressed size mismatch");
    }

    return output;
}
