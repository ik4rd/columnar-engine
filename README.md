# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-28 13:26:56` · `queries 43/43` · `warm runs 4` · [`csv`](benchmarks/results/readme_benchmarks.csv)

### Summary

| Metric | Value |
| --- | ---: |
| queries ok | 43 / 43 |
| queries failed | 0 |
| median warm time | 239.79 ms |
| average warm time | 274.57 ms |
| p95 warm time | 813.63 ms |
| cold/warm delta | 6.7% |
| total output size | 20.3 KB |
| max output size | 6.0 KB |
| fastest query | Q06 · 4.20 ms |
| slowest query | Q18 · 1146.61 ms |

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
| csv -> columnar | 5.52s |
| columnar -> csv | 9.65s |
| convert throughput | 145.4 MB/s |
| roundtrip throughput | 12.4 MB/s |

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 🟩 `5.33` | Q01 🟩 `7.51` | Q02 🟩 `18.46` | Q03 🟩 `14.22` | Q04 🟩 `142.67` | Q05 🟩 `127.92` |
| Q06 🟩 `4.20` | Q07 🟩 `6.58` | Q08 🟩 `184.89` | Q09 🟩 `200.71` | Q10 🟥 `266.04` | Q11 🟥 `308.16` |
| Q12 🟦 `228.27` | Q13 🟦 `244.72` | Q14 🟦 `229.88` | Q15 🟩 `69.88` | Q16 🟩 `139.03` | Q17 🟩 `149.92` |
| Q18 🟥 `1146.61` | Q19 🟩 `4.85` | Q20 🟩 `144.36` | Q21 🟥 `283.42` | Q22 🟥 `371.82` | Q23 🟦 `245.27` |
| Q24 🟥 `275.45` | Q25 🟥 `329.49` | Q26 🟥 `269.75` | Q27 🟥 `439.36` | Q28 🟥 `957.04` | Q29 🟥 `390.44` |
| Q30 🟦 `239.79` | Q31 🟦 `261.03` | Q32 🟥 `828.89` | Q33 🟥 `369.78` | Q34 🟥 `482.41` | Q35 🟥 `676.27` |
| Q36 🟥 `324.34` | Q37 🟦 `235.02` | Q38 🟩 `69.98` | Q39 🟥 `416.78` | Q40 🟩 `90.83` | Q41 🟩 `32.00` |
| Q42 🟥 `573.07` |  |  |  |  |  |
### Largest Result Sets

| Query | Output | Warm median, ms | SQL |
| --- | ---: | ---: | --- |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 371.82 | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT… |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 245.27 | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime… |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 390.44 | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(Reso… |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 416.78 | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (… |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 275.45 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY… |

### Fastest Queries

| Query | SQL | Warm median, ms | First run, ms |
| --- | --- | ---: | ---: |
| [Q06](benchmarks/queries/query_6.sql) | SELECT MIN(EventDate), MAX(EventDate) FROM hits; | 4.20 | 4.73 |
| [Q19](benchmarks/queries/query_19.sql) | SELECT UserID FROM hits WHERE UserID = 435090932899640449; | 4.85 | 5.58 |
| [Q00](benchmarks/queries/query_0.sql) | SELECT COUNT(*) FROM hits; | 5.33 | 10.91 |
| [Q07](benchmarks/queries/query_7.sql) | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY … | 6.58 | 6.60 |
| [Q01](benchmarks/queries/query_1.sql) | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0; | 7.51 | 14.12 |

### Slowest Queries

| Query | SQL | Warm median, ms | Warm max, ms |
| --- | --- | ---: | ---: |
| [Q18](benchmarks/queries/query_18.sql) | SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT… | 1146.61 | 1176.15 |
| [Q28](benchmarks/queries/query_28.sql) | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')… | 957.04 | 967.25 |
| [Q32](benchmarks/queries/query_32.sql) | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(Resolution… | 828.89 | 953.89 |
| [Q35](benchmarks/queries/query_35.sql) | SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS … | 676.27 | 678.94 |
| [Q42](benchmarks/queries/query_42.sql) | SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM… | 573.07 | 574.43 |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 10.91 | 5.73 | 5.33 | 5.06 | 7.21 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 14.12 | 7.41 | 7.51 | 6.92 | 7.72 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 27.96 | 18.37 | 18.46 | 17.43 | 19.11 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 24.73 | 14.17 | 14.22 | 13.58 | 14.67 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 142.46 | 143.75 | 142.67 | 142.39 | 147.26 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 137.41 | 127.97 | 127.92 | 127.70 | 128.33 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 4.73 | 4.21 | 4.20 | 4.11 | 4.35 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 6.60 | 6.58 | 6.58 | 6.55 | 6.64 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 187.86 | 185.13 | 184.89 | 184.61 | 186.12 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 199.68 | 200.89 | 200.71 | 200.13 | 202.00 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 276.16 | 266.05 | 266.04 | 263.92 | 268.23 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 276.72 | 309.33 | 308.16 | 267.94 | 353.04 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 410.85 | 229.06 | 228.27 | 225.02 | 234.66 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 243.61 | 249.32 | 244.72 | 239.30 | 268.53 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 228.50 | 230.86 | 229.88 | 226.83 | 236.84 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 74.99 | 69.83 | 69.88 | 69.21 | 70.34 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 123.59 | 135.96 | 139.03 | 119.77 | 146.01 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 186.73 | 152.43 | 149.92 | 145.57 | 164.29 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 1168.73 | 1151.55 | 1146.61 | 1136.85 | 1176.15 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 5.58 | 4.83 | 4.85 | 4.65 | 4.96 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 188.34 | 144.23 | 144.36 | 142.56 | 145.65 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 277.03 | 282.07 | 283.42 | 270.81 | 290.61 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 410.96 | 372.51 | 371.82 | 362.17 | 384.20 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 276.18 | 245.86 | 245.27 | 242.46 | 250.46 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 272.21 | 287.04 | 275.45 | 272.69 | 324.55 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 287.75 | 410.16 | 329.49 | 263.83 | 717.82 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 316.19 | 270.07 | 269.75 | 268.76 | 272.01 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 464.09 | 448.40 | 439.36 | 421.28 | 493.62 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 1003.27 | 958.87 | 957.04 | 954.14 | 967.25 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 395.02 | 390.10 | 390.44 | 387.37 | 392.13 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 256.21 | 238.89 | 239.79 | 235.62 | 240.37 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 299.84 | 261.53 | 261.03 | 258.93 | 265.14 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 986.27 | 854.79 | 828.89 | 807.48 | 953.89 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 378.58 | 374.65 | 369.78 | 363.09 | 395.96 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 473.26 | 480.15 | 482.41 | 408.94 | 546.83 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 676.69 | 675.28 | 676.27 | 669.64 | 678.94 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 340.63 | 324.75 | 324.34 | 321.71 | 328.61 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 249.18 | 234.70 | 235.02 | 233.13 | 235.62 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 75.87 | 70.08 | 69.98 | 69.94 | 70.44 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 489.41 | 416.60 | 416.78 | 408.36 | 424.48 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 106.21 | 91.04 | 90.83 | 90.28 | 92.23 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 37.17 | 32.00 | 32.00 | 31.92 | 32.07 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 580.31 | 572.39 | 573.07 | 568.98 | 574.43 | ok |
<!-- benchmark-table:end -->
