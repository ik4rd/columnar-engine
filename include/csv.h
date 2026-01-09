#pragma once

#include <istream>
#include <ostream>
#include <string>
#include <vector>

bool ReadCsvRow(std::istream& in, std::vector<std::string>& row);
void WriteCsvRow(std::ostream& out, const std::vector<std::string>& row);
