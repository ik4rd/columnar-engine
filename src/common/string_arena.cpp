#include "common/string_arena.h"

#include <algorithm>
#include <cstring>
#include <functional>

StringArena::StringArena(const size_t chunk_size) : chunk_size_(chunk_size) {}

std::string_view StringArena::Store(const std::string_view value) {
    if (value.empty()) {
        return {};
    }

    Chunk& chunk = EnsureChunk(value.size());
    const size_t offset = chunk.used;
    std::memcpy(chunk.data.get() + static_cast<std::ptrdiff_t>(offset), value.data(), value.size());
    chunk.used = offset + value.size();
    return {chunk.data.get() + static_cast<std::ptrdiff_t>(offset), value.size()};
}

void StringArena::Clear() {
    for (Chunk& chunk : chunks_) {
        chunk.used = 0;
    }
}

StringArena::Chunk& StringArena::EnsureChunk(const size_t size) {
    if (!chunks_.empty() && chunks_.back().capacity - chunks_.back().used >= size) {
        return chunks_.back();
    }

    const size_t capacity = std::max(chunk_size_, size);
    chunks_.push_back(Chunk{
        .data = std::make_unique<char[]>(capacity),
        .capacity = capacity,
        .used = 0,
    });

    return chunks_.back();
}

size_t StringViewHash::operator()(const std::string_view value) const noexcept {
    return std::hash<std::string_view>{}(value);
}

bool StringViewEqual::operator()(const std::string_view lhs, const std::string_view rhs) const noexcept {
    return lhs == rhs;
}
