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

inline ExecuteExpected RunQuery8(const Executor& executor) {
    return executor.Execute(
        "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC "
        "LIMIT 10;");
}

inline ExecuteExpected RunQuery9(const Executor& executor) {
    return executor.Execute(
        "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), "
        "COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery10(const Executor& executor) {
    return executor.Execute(
        "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE "
        "MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery11(const Executor& executor) {
    return executor.Execute(
        "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE "
        "MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery12(const Executor& executor) {
    return executor.Execute(
        "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY "
        "SearchPhrase ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery13(const Executor& executor) {
    return executor.Execute(
        "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' "
        "GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery14(const Executor& executor) {
    return executor.Execute(
        "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' "
        "GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery15(const Executor& executor) {
    return executor.Execute("SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery16(const Executor& executor) {
    return executor.Execute(
        "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC "
        "LIMIT 10;");
}

inline const std::map<int, QueryRunner>& GetQueryRunners() {
    static const std::map<int, QueryRunner> Runners = {
        {0, RunQuery0},   {1, RunQuery1},   {2, RunQuery2},   {3, RunQuery3},   {4, RunQuery4},   {5, RunQuery5},
        {6, RunQuery6},   {7, RunQuery7},   {8, RunQuery8},   {9, RunQuery9},   {10, RunQuery10}, {11, RunQuery11},
        {12, RunQuery12}, {13, RunQuery13}, {14, RunQuery14}, {15, RunQuery15}, {16, RunQuery16},
    };
    return Runners;
}
