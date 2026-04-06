#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include "fileio.h"

int main() {
    try {
        const std::filesystem::path path = std::filesystem::temp_directory_path() / "fileio_example.bin";

        const std::vector<uint8_t> payload = {1, 2, 3, 4};
        {
            std::ofstream out = OpenOutputFile(path);
            out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
            out.flush();
        }

        const std::vector<uint8_t> extra = {5, 6};
        {
            std::ofstream out = OpenOutputFile(path, FileOpenMode::Append);
            out.write(reinterpret_cast<const char*>(extra.data()), static_cast<std::streamsize>(extra.size()));
            out.flush();
        }

        std::ifstream in = OpenInputFile(path);
        std::vector<uint8_t> bytes(payload.size() + extra.size());
        in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));

        std::cout << "read bytes: " << bytes.size() << std::endl;
        if (const auto metadata = TryGetFileMetadata(path)) {
            std::cout << "size on disk: " << metadata->size << std::endl;
            std::cout << "is regular: " << std::boolalpha << metadata->is_regular << std::endl;
        }

        for (uint8_t byte : bytes) {
            std::cout << static_cast<int>(byte) << " ";
        }
        std::cout << std::endl;

        std::error_code ec;
        std::filesystem::remove(path, ec);
    } catch (const std::exception& ex) {
        std::cerr << "fileio example failed: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
