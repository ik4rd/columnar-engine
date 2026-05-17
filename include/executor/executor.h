#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/error.h"
#include "model/batch.h"
#include "tl/expected.hpp"

using ExecuteExpected = tl::expected<Batch, Error>;

class Executor {
   public:
    Executor() = default;

    void RegisterTable(const std::string& name, std::filesystem::path path);
    void SetUnsupportedFallbackEnabled(bool enabled);
    ExecuteExpected Execute(std::string_view query) const;

   private:
    std::unordered_map<std::string, std::filesystem::path> tables_;
    bool unsupported_fallback_enabled_ = false;
};
