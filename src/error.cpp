#include "error.h"

std::runtime_error MakeError(const std::string& module, const std::string& message) {
    return std::runtime_error(module + ": " + message);
}

std::runtime_error MakePathError(const std::string& module, const std::filesystem::path& path,
                                 const std::string& action) {
    return std::runtime_error(module + ": failed to " + action + " '" + path.string() + "'");
}
