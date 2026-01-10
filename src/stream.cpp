#include "stream.h"

#include <ios>

#include "error.h"

void ReadBytes(std::istream& in, char* dst, const size_t size) {
    if (size == 0) {
        return;
    }
    in.read(dst, static_cast<std::streamsize>(size));
    if (in.gcount() != static_cast<std::streamsize>(size) || !in) {
        throw error::MakeError("stream", "failed to read bytes");
    }
}

void WriteBytes(std::ostream& out, std::string_view bytes) {
    if (bytes.empty()) {
        return;
    }
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if (!out) {
        throw error::MakeError("stream", "failed to write bytes");
    }
}
