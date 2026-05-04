#include "query_runners.h"

ExecuteExpected RunQuery0(const Executor& executor) { return executor.Execute("SELECT COUNT(*) FROM hits;"); }

ExecuteExpected RunQuery1(const Executor& executor) {
    return executor.Execute("SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;");
}

ExecuteExpected RunQuery2(const Executor& executor) {
    return executor.Execute("SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;");
}

ExecuteExpected RunQuery3(const Executor& executor) { return executor.Execute("SELECT AVG(UserID) FROM hits;"); }

ExecuteExpected RunQuery4(const Executor& executor) {
    return executor.Execute("SELECT COUNT(DISTINCT UserID) FROM hits;");
}

const std::map<int, QueryRunner>& GetQueryRunners() {
    static const std::map<int, QueryRunner> Runners = {
        {0, RunQuery0}, {1, RunQuery1}, {2, RunQuery2}, {3, RunQuery3}, {4, RunQuery4},
    };
    return Runners;
}
