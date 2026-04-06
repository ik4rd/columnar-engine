#include "error.h"

std::runtime_error error::MakeError(const std::string& module, const std::string& message) {
    return std::runtime_error(module + ": " + message);
}
std::runtime_error error::MakePathError(const std::string& module, const std::filesystem::path& path,
                                        const std::string& action) {
    return std::runtime_error(module + ": failed to " + action + " '" + path.string() + "'");
}
