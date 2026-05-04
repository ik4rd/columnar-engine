#pragma once

#include <functional>
#include <map>

#include "executor/executor.h"

using QueryRunner = std::function<ExecuteExpected(const Executor&)>;

inline ExecuteExpected RunQuery0(const Executor& executor) { return executor.Execute("SELECT COUNT(*) FROM hits;"); }

inline ExecuteExpected RunQuery1(const Executor& executor) {
    return executor.Execute("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;");
}

inline ExecuteExpected RunQuery2(const Executor& executor) {
    return executor.Execute("SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;");
}

inline ExecuteExpected RunQuery3(const Executor& executor) { return executor.Execute("SELECT AVG(UserID) FROM hits;"); }

inline ExecuteExpected RunQuery4(const Executor& executor) {
    return executor.Execute("SELECT COUNT(DISTINCT UserID) FROM hits;");
}

inline ExecuteExpected RunQuery5(const Executor& executor) {
    return executor.Execute("SELECT COUNT(DISTINCT SearchPhrase) FROM hits;");
}

inline ExecuteExpected RunQuery6(const Executor& executor) {
    return executor.Execute("SELECT MIN(EventDate), MAX(EventDate) FROM hits;");
}

inline ExecuteExpected RunQuery7(const Executor& executor) {
    return executor.Execute(
        "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;");
}

inline const std::map<int, QueryRunner>& GetQueryRunners() {
    static const std::map<int, QueryRunner> Runners = {
        {0, RunQuery0},
        {1, RunQuery1},
        {2, RunQuery2},
        {3, RunQuery3},
        {4, RunQuery4},
        {5, RunQuery5},
        {6, RunQuery6},
        {7, RunQuery7},
    };
    return Runners;
}
