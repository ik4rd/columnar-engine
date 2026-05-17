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

inline ExecuteExpected RunQuery17(const Executor& executor) {
    return executor.Execute("SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;");
}

inline ExecuteExpected RunQuery18(const Executor& executor) {
    return executor.Execute(
        "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, "
        "SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery19(const Executor& executor) {
    return executor.Execute("SELECT UserID FROM hits WHERE UserID = 435090932899640449;");
}

inline ExecuteExpected RunQuery20(const Executor& executor) {
    return executor.Execute("SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';");
}

inline ExecuteExpected RunQuery21(const Executor& executor) {
    return executor.Execute(
        "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' "
        "GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery22(const Executor& executor) {
    return executor.Execute(
        "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE "
        "'%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT "
        "10;");
}

inline ExecuteExpected RunQuery23(const Executor& executor) {
    return executor.Execute("SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;");
}

inline ExecuteExpected RunQuery24(const Executor& executor) {
    return executor.Execute("SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;");
}

inline ExecuteExpected RunQuery25(const Executor& executor) {
    return executor.Execute("SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;");
}

inline ExecuteExpected RunQuery26(const Executor& executor) {
    return executor.Execute(
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;");
}

inline ExecuteExpected RunQuery27(const Executor& executor) {
    return executor.Execute(
        "SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING "
        "COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;");
}

inline ExecuteExpected RunQuery28(const Executor& executor) {
    return executor.Execute(
        R"(SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;)");
}

inline ExecuteExpected RunQuery29(const Executor& executor) {
    return executor.Execute(
        "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), "
        "SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), "
        "SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), "
        "SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), "
        "SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), "
        "SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), "
        "SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), "
        "SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), "
        "SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), "
        "SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), "
        "SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), "
        "SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), "
        "SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), "
        "SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), "
        "SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), "
        "SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), "
        "SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), "
        "SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), "
        "SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), "
        "SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), "
        "SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), "
        "SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), "
        "SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;");
}

inline ExecuteExpected RunQuery30(const Executor& executor) {
    return executor.Execute(
        "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE "
        "SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery31(const Executor& executor) {
    return executor.Execute(
        "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> "
        "'' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery32(const Executor& executor) {
    return executor.Execute(
        "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, "
        "ClientIP ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery33(const Executor& executor) {
    return executor.Execute("SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery34(const Executor& executor) {
    return executor.Execute("SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery35(const Executor& executor) {
    return executor.Execute(
        "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, "
        "ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery36(const Executor& executor) {
    return executor.Execute(
        "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate "
        "<= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC "
        "LIMIT 10;");
}

inline ExecuteExpected RunQuery37(const Executor& executor) {
    return executor.Execute(
        "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND "
        "EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY "
        "PageViews DESC LIMIT 10;");
}

inline ExecuteExpected RunQuery38(const Executor& executor) {
    return executor.Execute(
        "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate "
        "<= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC "
        "LIMIT 10 OFFSET 1000;");
}

inline ExecuteExpected RunQuery39(const Executor& executor) {
    return executor.Execute(
        "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN "
        "Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= "
        "'2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, "
        "AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;");
}

inline ExecuteExpected RunQuery40(const Executor& executor) {
    return executor.Execute(
        "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= "
        "'2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = "
        "3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;");
}

inline ExecuteExpected RunQuery41(const Executor& executor) {
    return executor.Execute(
        "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND "
        "EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = "
        "2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET "
        "10000;");
}

inline ExecuteExpected RunQuery42(const Executor& executor) {
    return executor.Execute(
        "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND "
        "EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY "
        "DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000;");
}

inline const std::map<int, QueryRunner>& GetQueryRunners() {
    static const std::map<int, QueryRunner> Runners = {
        {0, RunQuery0},   {1, RunQuery1},   {2, RunQuery2},   {3, RunQuery3},   {4, RunQuery4},   {5, RunQuery5},
        {6, RunQuery6},   {7, RunQuery7},   {8, RunQuery8},   {9, RunQuery9},   {10, RunQuery10}, {11, RunQuery11},
        {12, RunQuery12}, {13, RunQuery13}, {14, RunQuery14}, {15, RunQuery15}, {16, RunQuery16}, {17, RunQuery17},
        {18, RunQuery18}, {19, RunQuery19}, {20, RunQuery20}, {21, RunQuery21}, {22, RunQuery22}, {23, RunQuery23},
        {24, RunQuery24}, {25, RunQuery25}, {26, RunQuery26}, {27, RunQuery27}, {28, RunQuery28}, {29, RunQuery29},
        {30, RunQuery30}, {31, RunQuery31}, {32, RunQuery32}, {33, RunQuery33}, {34, RunQuery34}, {35, RunQuery35},
        {36, RunQuery36}, {37, RunQuery37}, {38, RunQuery38}, {39, RunQuery39}, {40, RunQuery40}, {41, RunQuery41},
        {42, RunQuery42},
    };
    return Runners;
}
