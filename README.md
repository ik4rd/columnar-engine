# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-28 03:03:29` · `queries 43/43` · `warm runs 4` · [`csv`](benchmarks/results/readme_benchmarks.csv)

### Summary

| Metric | Value |
| --- | ---: |
| queries ok | 43 / 43 |
| queries failed | 0 |
| median warm time | 237.07 ms |
| average warm time | 268.20 ms |
| p95 warm time | 777.08 ms |
| cold/warm delta | 11.3% |
| total output size | 20.3 KB |
| max output size | 6.0 KB |
| fastest query | Q06 · 3.68 ms |
| slowest query | Q18 · 1112.19 ms |

### Storage

| Metric | Value |
| --- | ---: |
| source csv | `benchmarks/hits_sample.csv` |
| schema | `benchmarks/scheme.csv` |
| source size | 802.6 MB |
| columnar size | 694.9 MB |
| roundtrip csv size | 743.0 MB |
| compression ratio | 1.16x |
| columnar / csv | 86.6% |
| csv -> columnar | 5.77s |
| columnar -> csv | 9.67s |
| convert throughput | 139.2 MB/s |
| roundtrip throughput | 71.9 MB/s |

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 🟩 `3.91` | Q01 🟩 `6.32` | Q02 🟩 `17.25` | Q03 🟩 `13.37` | Q04 🟩 `148.12` | Q05 🟩 `130.96` |
| Q06 🟩 `3.68` | Q07 🟩 `6.69` | Q08 🟩 `193.36` | Q09 🟩 `209.45` | Q10 🟥 `268.08` | Q11 🟥 `269.48` |
| Q12 🟦 `233.32` | Q13 🟦 `245.55` | Q14 🟦 `238.18` | Q15 🟩 `73.32` | Q16 🟩 `122.39` | Q17 🟩 `144.28` |
| Q18 🟥 `1112.19` | Q19 🟩 `4.11` | Q20 🟩 `134.97` | Q21 🟥 `268.11` | Q22 🟥 `357.07` | Q23 🟦 `236.84` |
| Q24 🟥 `273.73` | Q25 🟦 `226.36` | Q26 🟥 `289.97` | Q27 🟥 `398.98` | Q28 🟥 `907.32` | Q29 🟥 `414.52` |
| Q30 🟦 `237.07` | Q31 🟥 `262.05` | Q32 🟥 `787.69` | Q33 🟥 `419.25` | Q34 🟥 `423.99` | Q35 🟥 `681.60` |
| Q36 🟥 `337.69` | Q37 🟦 `244.01` | Q38 🟩 `67.28` | Q39 🟥 `426.31` | Q40 🟩 `92.18` | Q41 🟩 `33.18` |
| Q42 🟥 `568.52` |  |  |  |  |  |
### Largest Result Sets

| Query | Output | Warm median, ms | SQL |
| --- | ---: | ---: | --- |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 357.07 | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT… |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 236.84 | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime… |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 414.52 | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(Reso… |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 426.31 | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (… |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 273.73 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY… |

### Fastest Queries

| Query | SQL | Warm median, ms | First run, ms |
| --- | --- | ---: | ---: |
| [Q06](benchmarks/queries/query_6.sql) | SELECT MIN(EventDate), MAX(EventDate) FROM hits; | 3.68 | 4.43 |
| [Q00](benchmarks/queries/query_0.sql) | SELECT COUNT(*) FROM hits; | 3.91 | 8.14 |
| [Q19](benchmarks/queries/query_19.sql) | SELECT UserID FROM hits WHERE UserID = 435090932899640449; | 4.11 | 4.50 |
| [Q01](benchmarks/queries/query_1.sql) | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0; | 6.32 | 34.17 |
| [Q07](benchmarks/queries/query_7.sql) | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY … | 6.69 | 6.40 |

### Slowest Queries

| Query | SQL | Warm median, ms | Warm max, ms |
| --- | --- | ---: | ---: |
| [Q18](benchmarks/queries/query_18.sql) | SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT… | 1112.19 | 1178.59 |
| [Q28](benchmarks/queries/query_28.sql) | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')… | 907.32 | 910.64 |
| [Q32](benchmarks/queries/query_32.sql) | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(Resolution… | 787.69 | 790.48 |
| [Q35](benchmarks/queries/query_35.sql) | SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS … | 681.60 | 683.99 |
| [Q42](benchmarks/queries/query_42.sql) | SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM… | 568.52 | 569.02 |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 8.14 | 3.95 | 3.91 | 3.78 | 4.22 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 34.17 | 6.33 | 6.32 | 6.01 | 6.67 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 48.00 | 17.26 | 17.25 | 16.83 | 17.71 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 49.77 | 13.43 | 13.37 | 13.21 | 13.76 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 148.05 | 149.27 | 148.12 | 147.47 | 153.38 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 165.31 | 130.88 | 130.96 | 130.57 | 131.02 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 4.43 | 3.69 | 3.68 | 3.60 | 3.82 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 6.40 | 6.80 | 6.69 | 6.46 | 7.35 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 230.36 | 193.81 | 193.36 | 191.19 | 197.33 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 210.07 | 210.02 | 209.45 | 209.09 | 212.08 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 299.72 | 268.08 | 268.08 | 267.32 | 268.86 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 287.55 | 270.30 | 269.48 | 268.57 | 273.67 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 229.11 | 232.94 | 233.32 | 230.58 | 234.54 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 247.57 | 246.07 | 245.55 | 244.77 | 248.40 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 249.88 | 237.64 | 238.18 | 235.56 | 238.64 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 75.58 | 73.07 | 73.32 | 71.09 | 74.55 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 130.68 | 122.37 | 122.39 | 121.98 | 122.72 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 155.01 | 144.52 | 144.28 | 142.55 | 146.97 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 1288.46 | 1127.01 | 1112.19 | 1105.08 | 1178.59 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 4.50 | 4.34 | 4.11 | 4.01 | 5.15 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 227.21 | 135.07 | 134.97 | 134.86 | 135.50 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 266.42 | 268.70 | 268.11 | 264.90 | 273.70 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 471.23 | 356.98 | 357.07 | 354.97 | 358.80 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 272.00 | 236.75 | 236.84 | 235.63 | 237.68 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 280.44 | 274.08 | 273.73 | 272.01 | 276.87 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 226.93 | 226.56 | 226.36 | 226.06 | 227.44 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 272.76 | 300.66 | 289.97 | 271.68 | 351.01 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 449.09 | 398.29 | 398.98 | 394.65 | 400.55 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 1015.82 | 907.49 | 907.32 | 904.67 | 910.64 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 414.40 | 414.81 | 414.52 | 414.18 | 416.03 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 290.88 | 236.69 | 237.07 | 234.72 | 237.89 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 271.91 | 261.89 | 262.05 | 258.68 | 264.77 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 1001.96 | 785.02 | 787.69 | 774.24 | 790.48 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 391.70 | 454.24 | 419.25 | 390.48 | 587.97 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 436.48 | 424.10 | 423.99 | 413.11 | 435.32 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 689.66 | 681.69 | 681.60 | 679.59 | 683.99 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 379.10 | 338.62 | 337.69 | 333.24 | 345.84 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 256.58 | 244.32 | 244.01 | 243.86 | 245.39 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 83.42 | 67.53 | 67.28 | 67.05 | 68.52 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 536.54 | 426.07 | 426.31 | 423.11 | 428.55 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 113.53 | 92.15 | 92.18 | 91.78 | 92.46 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 46.85 | 33.59 | 33.18 | 32.37 | 35.65 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 572.27 | 568.26 | 568.52 | 566.96 | 569.02 | ok |
<!-- benchmark-table:end -->
