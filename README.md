# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | SQL | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| [query_0.sql](benchmarks/queries/query_0.sql) | `SELECT COUNT(*) FROM hits;` | 53.43 | 9.63 | 9.23 | 8.77 | 11.28 | ok |
| [query_1.sql](benchmarks/queries/query_1.sql) | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` | 41.22 | 27.88 | 27.79 | 27.43 | 28.51 | ok |
| [query_2.sql](benchmarks/queries/query_2.sql) | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` | 62.18 | 54.10 | 53.49 | 50.64 | 58.77 | ok |
| [query_3.sql](benchmarks/queries/query_3.sql) | `SELECT AVG(UserID) FROM hits;` | 85.12 | 58.40 | 54.68 | 54.23 | 70.00 | ok |
| [query_4.sql](benchmarks/queries/query_4.sql) | `SELECT COUNT(DISTINCT UserID) FROM hits;` | 46.61 | 47.05 | 47.30 | 43.96 | 49.62 | ok |
| [query_5.sql](benchmarks/queries/query_5.sql) | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` | 61.44 | 45.14 | 44.92 | 44.58 | 46.12 | ok |
| [query_6.sql](benchmarks/queries/query_6.sql) | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` | 685.92 | 665.72 | 665.60 | 665.26 | 666.43 | ok |
| [query_7.sql](benchmarks/queries/query_7.sql) | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE…` | 28.69 | 28.12 | 28.13 | 28.01 | 28.20 | ok |
<!-- benchmark-table:end -->
