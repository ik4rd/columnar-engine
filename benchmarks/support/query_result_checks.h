#pragma once

#include <string>
#include <string_view>

#include "model/batch.h"

std::string BatchToString(const Batch& batch);
bool QueryUsesGroupBy(std::string_view sql);
bool EqualAsRowMultisets(std::string_view lhs, std::string_view rhs);
