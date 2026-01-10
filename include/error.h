#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>

namespace error {
std::runtime_error MakeError(const std::string& module, const std::string& message);

std::runtime_error MakePathError(const std::string& module, const std::filesystem::path& path,
                                 const std::string& action);
}  // namespace error