#include "io/stream.h"

#include "common/error.h"

void ReadBytes(std::istream& in, char* dst, const size_t size) {
    if (size == 0) {
        return;
    }
    in.read(dst, size);
    if (in.gcount() != static_cast<std::streamsize>(size) || !in) {
        throw Error::Io("stream", "failed to read bytes");
    }
}

void WriteBytes(std::ostream& out, const std::string_view bytes) {
    if (bytes.empty()) {
        return;
    }
    out.write(bytes.data(), bytes.size());
    if (!out) {
        throw Error::Io("stream", "failed to write bytes");
    }
}
