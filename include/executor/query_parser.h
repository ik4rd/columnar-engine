#pragma once

#include <string_view>

#include "executor/query.h"

Query ParseQuery(std::string_view query);
