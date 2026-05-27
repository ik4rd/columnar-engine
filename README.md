# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard

_Generated at 2026-05-28 02:50:15 · queries: 43/43 · warm runs: 4 · source: [benchmarks/results/readme_benchmarks.csv](benchmarks/results/readme_benchmarks.csv)_

### Query KPIs

| Metric | Value |
| --- | ---: |
| Successful queries | 43 / 43 |
| Failed queries | 0 |
| Warm median | 231.57 ms |
| Warm average | 265.25 ms |
| Warm p95 | 758.14 ms |
| Cold start penalty | 10.1% |
| Total query output size | 20.3 KB |
| Largest query output | 6.0 KB |
| Fastest query | Q6 · 3.73 ms |
| Slowest query | Q18 · 1104.20 ms |

### CSV -> Columnar

| Metric | Value |
| --- | ---: |
| Source CSV | benchmarks/hits_sample.csv |
| Schema | benchmarks/scheme.csv |
| Source size | 802.6 MB |
| Columnar size | 694.9 MB |
| Roundtrip CSV size | 743.0 MB |
| Compression ratio | 1.16x |
| Columnar vs CSV | 86.6% |
| CSV -> columnar | 5.61s |
| Columnar -> CSV | 9.70s |
| Convert throughput | 143.1 MB/s |
| Roundtrip throughput | 71.6 MB/s |

### HEATMAP — время каждого запроса

Зелёный = быстрее медианы · Синий = около медианы · Красный = медленнее медианы · Серый = 0ms

| Q0 | Q1 | Q2 | Q3 | Q4 | Q5 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟩 4.05 ms | 🟩 6.24 ms | 🟩 17.08 ms | 🟩 13.28 ms | 🟩 150.21 ms | 🟩 130.26 ms |

| Q6 | Q7 | Q8 | Q9 | Q10 | Q11 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟩 3.73 ms | 🟩 6.59 ms | 🟩 193.76 ms | 🟦 209.17 ms | 🟥 267.61 ms | 🟥 268.53 ms |

| Q12 | Q13 | Q14 | Q15 | Q16 | Q17 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟦 227.91 ms | 🟦 243.64 ms | 🟦 231.57 ms | 🟩 72.76 ms | 🟩 122.89 ms | 🟩 145.43 ms |

| Q18 | Q19 | Q20 | Q21 | Q22 | Q23 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟥 1104.20 ms | 🟩 4.08 ms | 🟩 134.28 ms | 🟥 265.77 ms | 🟥 353.26 ms | 🟦 231.47 ms |

| Q24 | Q25 | Q26 | Q27 | Q28 | Q29 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟥 279.29 ms | 🟦 226.32 ms | 🟥 271.74 ms | 🟥 395.00 ms | 🟥 910.04 ms | 🟥 413.93 ms |

| Q30 | Q31 | Q32 | Q33 | Q34 | Q35 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟦 234.69 ms | 🟥 258.50 ms | 🟥 766.79 ms | 🟥 373.11 ms | 🟥 413.90 ms | 🟥 680.33 ms |

| Q36 | Q37 | Q38 | Q39 | Q40 | Q41 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟥 334.59 ms | 🟦 242.30 ms | 🟩 66.95 ms | 🟥 439.02 ms | 🟩 92.05 ms | 🟩 32.32 ms |

| Q42 |
| ---: |
| 🟥 567.10 ms |

### Largest outputs

| Query | Output CSV | SQL | Warm median, ms |
| --- | ---: | --- | ---: |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT… | 353.26 |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime… | 231.47 |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(Reso… | 413.93 |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (… | 439.02 |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY… | 279.29 |

### Fastest queries

| Query | SQL | Warm median, ms | First run, ms |
| --- | --- | ---: | ---: |
| [Q6](benchmarks/queries/query_6.sql) | SELECT MIN(EventDate), MAX(EventDate) FROM hits; | 3.73 | 4.81 |
| [Q0](benchmarks/queries/query_0.sql) | SELECT COUNT(*) FROM hits; | 4.05 | 5.07 |
| [Q19](benchmarks/queries/query_19.sql) | SELECT UserID FROM hits WHERE UserID = 435090932899640449; | 4.08 | 5.44 |
| [Q1](benchmarks/queries/query_1.sql) | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0; | 6.24 | 30.68 |
| [Q7](benchmarks/queries/query_7.sql) | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY … | 6.59 | 6.34 |

### Slowest queries

| Query | SQL | Warm median, ms | Warm max, ms |
| --- | --- | ---: | ---: |
| [Q18](benchmarks/queries/query_18.sql) | SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT… | 1104.20 | 1136.09 |
| [Q28](benchmarks/queries/query_28.sql) | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')… | 910.04 | 917.66 |
| [Q32](benchmarks/queries/query_32.sql) | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(Resolution… | 766.79 | 1054.41 |
| [Q35](benchmarks/queries/query_35.sql) | SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS … | 680.33 | 684.33 |
| [Q42](benchmarks/queries/query_42.sql) | SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM… | 567.10 | 569.04 |

### All queries

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q0](benchmarks/queries/query_0.sql) | 7 B | 5.07 | 4.21 | 4.05 | 3.89 | 4.87 | ok |
| [Q1](benchmarks/queries/query_1.sql) | 6 B | 30.68 | 6.27 | 6.24 | 5.94 | 6.67 | ok |
| [Q2](benchmarks/queries/query_2.sql) | 18 B | 44.52 | 17.06 | 17.08 | 16.69 | 17.42 | ok |
| [Q3](benchmarks/queries/query_3.sql) | 20 B | 47.42 | 13.31 | 13.28 | 13.10 | 13.59 | ok |
| [Q4](benchmarks/queries/query_4.sql) | 6 B | 147.63 | 152.91 | 150.21 | 145.67 | 165.53 | ok |
| [Q5](benchmarks/queries/query_5.sql) | 6 B | 177.50 | 130.35 | 130.26 | 129.74 | 131.15 | ok |
| [Q6](benchmarks/queries/query_6.sql) | 22 B | 4.81 | 3.76 | 3.73 | 3.59 | 3.99 | ok |
| [Q7](benchmarks/queries/query_7.sql) | 31 B | 6.34 | 6.74 | 6.59 | 6.33 | 7.44 | ok |
| [Q8](benchmarks/queries/query_8.sql) | 81 B | 224.83 | 193.89 | 193.76 | 193.44 | 194.61 | ok |
| [Q9](benchmarks/queries/query_9.sql) | 237 B | 208.45 | 209.53 | 209.17 | 208.45 | 211.33 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 297.64 | 267.50 | 267.61 | 266.46 | 268.31 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 283.92 | 268.63 | 268.53 | 266.87 | 270.59 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 228.50 | 228.14 | 227.91 | 227.73 | 229.03 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 243.71 | 243.76 | 243.64 | 243.45 | 244.30 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 247.61 | 231.57 | 231.57 | 231.42 | 231.74 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 73.64 | 72.67 | 72.76 | 71.43 | 73.73 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 126.80 | 122.83 | 122.89 | 121.34 | 124.18 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 142.34 | 145.32 | 145.43 | 143.52 | 146.91 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 1179.00 | 1111.09 | 1104.20 | 1099.89 | 1136.09 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 5.44 | 4.11 | 4.08 | 3.89 | 4.37 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 218.86 | 134.30 | 134.28 | 133.76 | 134.87 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 265.42 | 265.43 | 265.77 | 263.81 | 266.37 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 457.09 | 354.85 | 353.26 | 352.40 | 360.49 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 274.95 | 231.74 | 231.47 | 229.01 | 235.01 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 278.10 | 277.66 | 279.29 | 270.98 | 281.07 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 226.27 | 226.58 | 226.32 | 225.83 | 227.85 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 274.29 | 271.82 | 271.74 | 271.70 | 272.10 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 416.35 | 398.32 | 395.00 | 391.86 | 411.43 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 1036.14 | 910.35 | 910.04 | 903.64 | 917.66 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 413.49 | 413.88 | 413.93 | 412.48 | 415.19 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 296.80 | 234.69 | 234.69 | 233.92 | 235.44 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 272.37 | 258.66 | 258.50 | 257.56 | 260.08 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 998.59 | 836.78 | 766.79 | 759.12 | 1054.41 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 381.56 | 372.90 | 373.11 | 369.05 | 376.36 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 403.20 | 413.64 | 413.90 | 402.06 | 424.72 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 682.04 | 681.13 | 680.33 | 679.51 | 684.33 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 363.09 | 333.69 | 334.59 | 330.17 | 335.41 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 253.24 | 242.43 | 242.30 | 240.69 | 244.44 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 81.23 | 67.02 | 66.95 | 66.83 | 67.32 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 513.37 | 496.03 | 439.02 | 423.81 | 682.29 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 109.06 | 92.02 | 92.05 | 91.62 | 92.36 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 46.91 | 32.30 | 32.32 | 31.92 | 32.65 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 569.64 | 567.37 | 567.10 | 566.27 | 569.04 | ok |
<!-- benchmark-table:end -->
