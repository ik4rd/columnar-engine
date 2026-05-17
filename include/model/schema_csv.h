#pragma once

#include <filesystem>

#include "model/schema.h"

Schema ReadSchemaCsv(const std::filesystem::path& path);
Schema InferSchemaCsv(const std::filesystem::path& path);
void WriteSchemaCsv(const std::filesystem::path& path, const Schema& schema);
