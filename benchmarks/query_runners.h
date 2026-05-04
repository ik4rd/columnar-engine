#pragma once

#include <functional>
#include <map>

#include "executor.h"

using QueryRunner = std::function<ExecuteExpected(const Executor&)>;

ExecuteExpected RunQuery0(const Executor& executor);
ExecuteExpected RunQuery1(const Executor& executor);
ExecuteExpected RunQuery2(const Executor& executor);
ExecuteExpected RunQuery3(const Executor& executor);
ExecuteExpected RunQuery4(const Executor& executor);

const std::map<int, QueryRunner>& GetQueryRunners();
