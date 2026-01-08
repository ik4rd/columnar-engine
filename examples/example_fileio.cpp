#include <filesystem>
#include <iostream>
#include <vector>

#include "fileio.h"

int main() {
    try {
        const std::filesystem::path path = std::filesystem::temp_directory_path() / "fileio_example.bin";

        const std::vector<std::uint8_t> payload = {1, 2, 3, 4};
        WriteFileBytes(path, payload);
        AppendFileBytes(path, std::vector<std::uint8_t>{5, 6});

        const auto bytes = ReadFileBytes(path);
        std::cout << "read bytes: " << bytes.size() << std::endl;

        if (const auto metadata = GetFileMetadata(path)) {
            std::cout << "size on disk: " << metadata->size << std::endl;
            std::cout << "is regular: " << std::boolalpha << metadata->is_regular << std::endl;
        }

        std::error_code ec;
        std::filesystem::remove(path, ec);
    } catch (const std::exception& ex) {
        std::cerr << "fileio example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
