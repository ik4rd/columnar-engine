# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-28 14:48:45` · `queries 43/43` · `warm runs 4` · [`csv`](benchmarks/results/readme_benchmarks.csv)

### Summary

| Metric | Value |
| --- | ---: |
| queries ok | 43 / 43 |
| queries failed | 0 |
| median warm time | 225.66 ms |
| average warm time | 221.06 ms |
| p95 warm time | 422.19 ms |
| cold/warm delta | 8.0% |
| total output size | 20.3 KB |
| max output size | 6.0 KB |
| fastest query | Q19 · 4.33 ms |
| slowest query | Q28 · 947.30 ms |

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
| csv -> columnar | 5.51s |
| columnar -> csv | 9.40s |
| convert throughput | 145.8 MB/s |
| roundtrip throughput | 12.7 MB/s |

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 🟩 `4.66` | Q01 🟩 `6.11` | Q02 🟩 `17.51` | Q03 🟩 `15.12` | Q04 🟩 `151.46` | Q05 🟩 `131.94` |
| Q06 🟩 `4.54` | Q07 🟩 `7.43` | Q08 🟩 `199.65` | Q09 🟦 `214.43` | Q10 🟥 `264.49` | Q11 🟥 `270.13` |
| Q12 🟦 `231.61` | Q13 🟦 `238.39` | Q14 🟦 `225.66` | Q15 🟩 `70.48` | Q16 🟩 `119.72` | Q17 🟩 `140.51` |
| Q18 🟥 `336.95` | Q19 🟩 `4.33` | Q20 🟩 `141.07` | Q21 🟥 `271.62` | Q22 🟥 `365.63` | Q23 🟦 `241.79` |
| Q24 🟥 `268.95` | Q25 🟦 `220.43` | Q26 🟥 `269.25` | Q27 🟥 `416.50` | Q28 🟥 `947.30` | Q29 🟥 `382.06` |
| Q30 🟦 `230.35` | Q31 🟥 `253.73` | Q32 🟥 `704.87` | Q33 🟥 `365.28` | Q34 🟥 `384.90` | Q35 🟩 `143.84` |
| Q36 🟥 `321.13` | Q37 🟦 `235.24` | Q38 🟩 `72.92` | Q39 🟥 `422.82` | Q40 🟩 `90.24` | Q41 🟩 `32.69` |
| Q42 🟩 `67.86` |  |  |  |  |  |
### Largest Result Sets

| Query | Output | Warm median, ms | SQL |
| --- | ---: | ---: | --- |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 365.63 | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT… |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 241.79 | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime… |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 382.06 | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(Reso… |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 422.82 | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (… |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 268.95 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY… |

### Fastest Queries

| Query | SQL | Warm median, ms | First run, ms |
| --- | --- | ---: | ---: |
| [Q19](benchmarks/queries/query_19.sql) | SELECT UserID FROM hits WHERE UserID = 435090932899640449; | 4.33 | 5.07 |
| [Q06](benchmarks/queries/query_6.sql) | SELECT MIN(EventDate), MAX(EventDate) FROM hits; | 4.54 | 5.32 |
| [Q00](benchmarks/queries/query_0.sql) | SELECT COUNT(*) FROM hits; | 4.66 | 32.52 |
| [Q01](benchmarks/queries/query_1.sql) | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0; | 6.11 | 15.03 |
| [Q07](benchmarks/queries/query_7.sql) | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY … | 7.43 | 6.70 |

### Slowest Queries

| Query | SQL | Warm median, ms | Warm max, ms |
| --- | --- | ---: | ---: |
| [Q28](benchmarks/queries/query_28.sql) | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')… | 947.30 | 947.70 |
| [Q32](benchmarks/queries/query_32.sql) | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(Resolution… | 704.87 | 709.81 |
| [Q39](benchmarks/queries/query_39.sql) | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEn… | 422.82 | 433.09 |
| [Q27](benchmarks/queries/query_27.sql) | SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE … | 416.50 | 418.36 |
| [Q34](benchmarks/queries/query_34.sql) | SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC … | 384.90 | 387.53 |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 32.52 | 4.63 | 4.66 | 4.45 | 4.74 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 15.03 | 6.21 | 6.11 | 6.08 | 6.53 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 27.26 | 17.87 | 17.51 | 17.33 | 19.14 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 28.56 | 14.98 | 15.12 | 13.54 | 16.16 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 149.26 | 180.46 | 151.46 | 142.19 | 276.73 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 150.42 | 132.22 | 131.94 | 131.34 | 133.66 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 5.32 | 4.58 | 4.54 | 4.21 | 5.05 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 6.70 | 7.46 | 7.43 | 7.19 | 7.81 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 201.60 | 199.34 | 199.65 | 194.41 | 203.66 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 212.51 | 218.14 | 214.43 | 210.86 | 232.86 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 276.50 | 264.53 | 264.49 | 263.64 | 265.52 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 267.70 | 271.02 | 270.13 | 265.75 | 278.06 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 233.29 | 249.14 | 231.61 | 228.06 | 305.29 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 253.15 | 238.86 | 238.39 | 237.53 | 241.14 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 225.86 | 225.66 | 225.66 | 225.40 | 225.93 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 70.78 | 70.34 | 70.48 | 69.77 | 70.64 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 119.70 | 119.67 | 119.72 | 119.02 | 120.24 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 139.96 | 140.43 | 140.51 | 139.65 | 141.07 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 402.60 | 336.78 | 336.95 | 334.78 | 338.43 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 5.07 | 4.35 | 4.33 | 4.26 | 4.49 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 186.80 | 141.09 | 141.07 | 140.79 | 141.45 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 271.28 | 271.51 | 271.62 | 270.68 | 272.11 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 408.96 | 366.24 | 365.63 | 362.18 | 371.51 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 279.14 | 242.44 | 241.79 | 240.90 | 245.29 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 270.04 | 269.01 | 268.95 | 268.55 | 269.59 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 220.27 | 220.38 | 220.43 | 219.87 | 220.80 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 270.29 | 270.39 | 269.25 | 268.48 | 274.56 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 420.06 | 416.45 | 416.50 | 414.43 | 418.36 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 972.67 | 947.10 | 947.30 | 946.08 | 947.70 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 386.30 | 383.47 | 382.06 | 381.66 | 388.12 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 232.60 | 230.63 | 230.35 | 228.93 | 232.88 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 269.77 | 260.09 | 253.73 | 250.18 | 282.74 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 1062.67 | 704.38 | 704.87 | 697.97 | 709.81 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 368.34 | 365.48 | 365.28 | 364.19 | 367.15 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 384.09 | 385.42 | 384.90 | 384.34 | 387.53 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 144.54 | 143.79 | 143.84 | 142.83 | 144.66 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 329.99 | 320.62 | 321.13 | 318.63 | 321.57 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 236.91 | 235.07 | 235.24 | 233.68 | 236.12 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 79.61 | 73.12 | 72.92 | 70.87 | 75.78 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 436.78 | 424.89 | 422.82 | 420.83 | 433.09 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 106.64 | 90.29 | 90.24 | 90.09 | 90.58 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 37.17 | 32.69 | 32.69 | 32.52 | 32.87 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 67.63 | 67.83 | 67.86 | 66.65 | 68.96 | ok |
<!-- benchmark-table:end -->
