#pragma once

#include <filesystem>
#include <istream>
#include <ostream>
#include <string>
#include <vector>

bool ReadCsvRow(std::istream& in, std::vector<std::string>& row);
void WriteCsvRow(std::ostream& out, const std::vector<std::string>& row);

std::vector<std::vector<std::string>> ReadRows(const std::filesystem::path& path);
void WriteRows(const std::filesystem::path& path, const std::vector<std::vector<std::string>>& rows);
