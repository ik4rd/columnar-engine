#pragma once

#include <cstddef>
#include <memory>
#include <string_view>
#include <unordered_set>
#include <vector>

class StringArena {
   public:
    explicit StringArena(size_t chunk_size = 4096);

    std::string_view Store(std::string_view value);
    void Clear();

   private:
    struct Chunk {
        std::unique_ptr<char[]> data;
        size_t capacity = 0;
        size_t used = 0;
    };

    Chunk& EnsureChunk(size_t size);

    size_t chunk_size_ = 0;
    std::vector<Chunk> chunks_;
};

struct StringViewHash {
    size_t operator()(std::string_view value) const noexcept;
};

struct StringViewEqual {
    bool operator()(std::string_view lhs, std::string_view rhs) const noexcept;
};

using StringViewSet = std::unordered_set<std::string_view, StringViewHash, StringViewEqual>;
