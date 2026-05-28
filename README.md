# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-28 14:14:38` · `queries 43/43` · `warm runs 4` · [`csv`](benchmarks/results/readme_benchmarks.csv)

### Summary

| Metric | Value |
| --- | ---: |
| queries ok | 43 / 43 |
| queries failed | 0 |
| median warm time | 227.39 ms |
| average warm time | 227.85 ms |
| p95 warm time | 469.27 ms |
| cold/warm delta | 4.1% |
| total output size | 20.3 KB |
| max output size | 6.0 KB |
| fastest query | Q19 · 4.30 ms |
| slowest query | Q28 · 961.46 ms |

### Storage

| Metric | Value |
| --- | ---: |
| source csv | `benchmarks/hits_sample.csv` |
| schema | `benchmarks/scheme.csv` |
| compression | `lz4` |
| source size | 802.6 MB |
| columnar size | 119.7 MB |
| roundtrip csv size | 743.0 MB |
| compression ratio | 6.71x |
| columnar / csv | 14.9% |
| csv -> columnar | 5.65s |
| columnar -> csv | 9.55s |
| convert throughput | 142.2 MB/s |
| roundtrip throughput | 12.5 MB/s |

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 🟩 `5.66` | Q01 🟩 `8.52` | Q02 🟩 `18.69` | Q03 🟩 `15.81` | Q04 🟩 `167.19` | Q05 🟩 `142.57` |
| Q06 🟩 `4.81` | Q07 🟩 `8.51` | Q08 🟩 `191.42` | Q09 🟦 `204.68` | Q10 🟥 `266.03` | Q11 🟥 `267.35` |
| Q12 🟦 `225.01` | Q13 🟦 `239.70` | Q14 🟦 `228.39` | Q15 🟩 `70.37` | Q16 🟩 `120.24` | Q17 🟩 `141.30` |
| Q18 🟥 `334.36` | Q19 🟩 `4.30` | Q20 🟩 `142.30` | Q21 🟥 `273.15` | Q22 🟥 `365.64` | Q23 🟦 `242.96` |
| Q24 🟥 `271.73` | Q25 🟦 `222.25` | Q26 🟥 `271.67` | Q27 🟥 `420.53` | Q28 🟥 `961.46` | Q29 🟥 `390.66` |
| Q30 🟦 `232.35` | Q31 🟥 `254.79` | Q32 🟥 `727.78` | Q33 🟥 `369.53` | Q34 🟥 `408.60` | Q35 🟦 `227.39` |
| Q36 🟥 `352.00` | Q37 🟦 `240.26` | Q38 🟩 `82.36` | Q39 🟥 `474.69` | Q40 🟩 `96.72` | Q41 🟩 `35.08` |
| Q42 🟩 `68.79` |  |  |  |  |  |
### Largest Result Sets

| Query | Output | Warm median, ms | SQL |
| --- | ---: | ---: | --- |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 365.64 | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT… |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 242.96 | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime… |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 390.66 | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(Reso… |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 474.69 | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (… |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 271.73 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY… |

### Fastest Queries

| Query | SQL | Warm median, ms | First run, ms |
| --- | --- | ---: | ---: |
| [Q19](benchmarks/queries/query_19.sql) | SELECT UserID FROM hits WHERE UserID = 435090932899640449; | 4.30 | 5.03 |
| [Q06](benchmarks/queries/query_6.sql) | SELECT MIN(EventDate), MAX(EventDate) FROM hits; | 4.81 | 4.83 |
| [Q00](benchmarks/queries/query_0.sql) | SELECT COUNT(*) FROM hits; | 5.66 | 6.73 |
| [Q07](benchmarks/queries/query_7.sql) | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY … | 8.51 | 11.26 |
| [Q01](benchmarks/queries/query_1.sql) | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0; | 8.52 | 7.83 |

### Slowest Queries

| Query | SQL | Warm median, ms | Warm max, ms |
| --- | --- | ---: | ---: |
| [Q28](benchmarks/queries/query_28.sql) | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')… | 961.46 | 965.33 |
| [Q32](benchmarks/queries/query_32.sql) | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(Resolution… | 727.78 | 799.88 |
| [Q39](benchmarks/queries/query_39.sql) | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEn… | 474.69 | 479.44 |
| [Q27](benchmarks/queries/query_27.sql) | SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE … | 420.53 | 423.62 |
| [Q34](benchmarks/queries/query_34.sql) | SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC … | 408.60 | 497.95 |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 6.73 | 5.73 | 5.66 | 5.04 | 6.58 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 7.83 | 8.39 | 8.52 | 7.40 | 9.11 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 25.17 | 18.62 | 18.69 | 18.22 | 18.89 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 14.89 | 16.28 | 15.81 | 14.92 | 18.60 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 169.19 | 166.66 | 167.19 | 160.12 | 172.16 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 158.88 | 142.99 | 142.57 | 139.13 | 147.68 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 4.83 | 4.81 | 4.81 | 4.51 | 5.09 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 11.26 | 9.06 | 8.51 | 8.09 | 11.12 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 220.05 | 191.69 | 191.42 | 188.73 | 195.20 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 205.60 | 204.66 | 204.68 | 204.17 | 205.11 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 273.84 | 266.00 | 266.03 | 265.52 | 266.41 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 268.04 | 267.38 | 267.35 | 267.27 | 267.58 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 224.63 | 224.93 | 225.01 | 224.44 | 225.28 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 240.61 | 239.63 | 239.70 | 238.61 | 240.50 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 228.67 | 228.32 | 228.39 | 227.04 | 229.47 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 71.09 | 70.54 | 70.37 | 69.94 | 71.46 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 122.74 | 120.44 | 120.24 | 119.31 | 121.99 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 141.73 | 141.49 | 141.30 | 140.31 | 143.04 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 369.08 | 335.98 | 334.36 | 333.21 | 342.01 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 5.03 | 4.30 | 4.30 | 4.27 | 4.34 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 143.32 | 142.33 | 142.30 | 142.00 | 142.71 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 271.64 | 272.93 | 273.15 | 271.58 | 273.83 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 366.39 | 365.99 | 365.64 | 364.45 | 368.23 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 249.63 | 242.91 | 242.96 | 242.01 | 243.72 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 271.33 | 271.81 | 271.73 | 271.32 | 272.46 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 223.93 | 222.37 | 222.25 | 221.78 | 223.21 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 271.34 | 271.66 | 271.67 | 271.41 | 271.90 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 421.81 | 421.00 | 420.53 | 419.31 | 423.62 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 966.80 | 961.41 | 961.46 | 957.40 | 965.33 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 396.16 | 390.69 | 390.66 | 387.70 | 393.73 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 236.87 | 232.38 | 232.35 | 232.17 | 232.65 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 262.18 | 254.79 | 254.79 | 252.74 | 256.84 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 967.89 | 741.95 | 727.78 | 712.38 | 799.88 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 368.98 | 380.07 | 369.53 | 362.34 | 418.87 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 386.18 | 427.59 | 408.60 | 395.22 | 497.95 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 149.27 | 215.79 | 227.39 | 164.34 | 244.05 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 367.61 | 349.09 | 352.00 | 328.04 | 364.33 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 255.09 | 253.91 | 240.26 | 238.20 | 296.91 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 78.61 | 81.87 | 82.36 | 72.82 | 89.95 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 546.84 | 468.97 | 474.69 | 447.05 | 479.44 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 113.76 | 96.55 | 96.72 | 94.74 | 98.03 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 39.03 | 35.20 | 35.08 | 34.74 | 35.88 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 78.57 | 68.56 | 68.79 | 67.35 | 69.30 | ok |
<!-- benchmark-table:end -->
