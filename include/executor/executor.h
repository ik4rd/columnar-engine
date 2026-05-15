#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>

#include "model/batch.h"
#include "support/error.h"
#include "tl/expected.hpp"

using ExecuteExpected = tl::expected<Batch, Error>;

class Executor {
   public:
    Executor() = default;

   public:
    void RegisterTable(const std::string& name, std::filesystem::path path);
    ExecuteExpected Execute(std::string_view query) const;

   private:
    std::unordered_map<std::string, std::filesystem::path> tables_;
};
