# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-28 02:56:43` · `queries 43/43` · `warm runs 4` · [`csv`](benchmarks/results/readme_benchmarks.csv)

### Summary

| Metric | Value |
| --- | ---: |
| queries ok | 43 / 43 |
| queries failed | 0 |
| median warm time | 236.19 ms |
| average warm time | 269.51 ms |
| p95 warm time | 781.88 ms |
| cold/warm delta | 10.2% |
| total output size | 20.3 KB |
| max output size | 6.0 KB |
| fastest query | Q06 · 3.78 ms |
| slowest query | Q18 · 1105.69 ms |

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
| csv -> columnar | 5.61s |
| columnar -> csv | 9.68s |
| convert throughput | 143.1 MB/s |
| roundtrip throughput | 71.8 MB/s |

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms

| Q00 | Q01 | Q02 | Q03 | Q04 | Q05 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟩 `4.05` | 🟩 `6.04` | 🟩 `17.38` | 🟩 `13.52` | 🟩 `149.66` | 🟩 `130.25` |

| Q06 | Q07 | Q08 | Q09 | Q10 | Q11 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟩 `3.78` | 🟩 `7.04` | 🟩 `193.67` | 🟩 `211.22` | 🟥 `267.17` | 🟥 `268.16` |

| Q12 | Q13 | Q14 | Q15 | Q16 | Q17 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟦 `231.33` | 🟦 `243.53` | 🟦 `231.03` | 🟩 `71.69` | 🟩 `122.41` | 🟩 `166.28` |

| Q18 | Q19 | Q20 | Q21 | Q22 | Q23 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟥 `1105.69` | 🟩 `4.06` | 🟩 `135.24` | 🟥 `266.05` | 🟥 `356.50` | 🟥 `273.90` |

| Q24 | Q25 | Q26 | Q27 | Q28 | Q29 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟥 `275.05` | 🟦 `225.91` | 🟥 `279.37` | 🟥 `392.98` | 🟥 `909.83` | 🟥 `416.26` |

| Q30 | Q31 | Q32 | Q33 | Q34 | Q35 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟦 `236.19` | 🟦 `258.87` | 🟥 `793.38` | 🟥 `422.04` | 🟥 `425.24` | 🟥 `678.43` |

| Q36 | Q37 | Q38 | Q39 | Q40 | Q41 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 🟥 `333.70` | 🟦 `243.18` | 🟩 `67.47` | 🟥 `455.30` | 🟩 `94.34` | 🟩 `32.35` |

| Q42 |
| ---: |
| 🟥 `569.60` |

### Largest Result Sets

| Query | Output | Warm median, ms | SQL |
| --- | ---: | --- | ---: |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 356.50 | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT… |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 273.90 | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime… |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 416.26 | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(Reso… |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 455.30 | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (… |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 275.05 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY… |

### Fastest Queries

| Query | SQL | Warm median, ms | First run, ms |
| --- | --- | ---: | ---: |
| [Q06](benchmarks/queries/query_6.sql) | SELECT MIN(EventDate), MAX(EventDate) FROM hits; | 3.78 | 4.29 |
| [Q00](benchmarks/queries/query_0.sql) | SELECT COUNT(*) FROM hits; | 4.05 | 8.44 |
| [Q19](benchmarks/queries/query_19.sql) | SELECT UserID FROM hits WHERE UserID = 435090932899640449; | 4.06 | 5.98 |
| [Q01](benchmarks/queries/query_1.sql) | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0; | 6.04 | 33.49 |
| [Q07](benchmarks/queries/query_7.sql) | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY … | 7.04 | 6.40 |

### Slowest Queries

| Query | SQL | Warm median, ms | Warm max, ms |
| --- | --- | ---: | ---: |
| [Q18](benchmarks/queries/query_18.sql) | SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT… | 1105.69 | 1114.97 |
| [Q28](benchmarks/queries/query_28.sql) | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1')… | 909.83 | 1040.04 |
| [Q32](benchmarks/queries/query_32.sql) | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(Resolution… | 793.38 | 862.25 |
| [Q35](benchmarks/queries/query_35.sql) | SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS … | 678.43 | 682.40 |
| [Q42](benchmarks/queries/query_42.sql) | SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM… | 569.60 | 573.88 |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 8.44 | 4.04 | 4.05 | 3.84 | 4.21 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 33.49 | 6.22 | 6.04 | 5.80 | 7.01 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 48.32 | 17.48 | 17.38 | 17.20 | 17.95 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 51.36 | 13.49 | 13.52 | 13.05 | 13.88 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 149.10 | 154.94 | 149.66 | 146.75 | 173.69 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 198.85 | 133.66 | 130.25 | 129.77 | 144.36 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 4.29 | 3.81 | 3.78 | 3.66 | 4.00 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 6.40 | 7.05 | 7.04 | 6.74 | 7.40 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 230.39 | 193.51 | 193.67 | 192.14 | 194.58 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 210.69 | 210.91 | 211.22 | 208.87 | 212.34 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 300.25 | 267.20 | 267.17 | 266.75 | 267.72 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 284.57 | 268.45 | 268.16 | 267.80 | 269.69 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 228.01 | 231.10 | 231.33 | 228.41 | 233.32 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 244.46 | 243.74 | 243.53 | 243.06 | 244.83 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 246.89 | 230.96 | 231.03 | 230.24 | 231.55 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 74.77 | 71.87 | 71.69 | 71.42 | 72.68 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 126.78 | 122.55 | 122.41 | 121.85 | 123.52 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 147.47 | 163.48 | 166.28 | 149.41 | 171.95 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 1169.47 | 1105.87 | 1105.69 | 1097.13 | 1114.97 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 5.98 | 4.10 | 4.06 | 3.92 | 4.34 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 227.08 | 135.18 | 135.24 | 134.62 | 135.59 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 265.75 | 266.15 | 266.05 | 263.98 | 268.53 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 474.08 | 356.36 | 356.50 | 352.93 | 359.50 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 276.26 | 283.14 | 273.90 | 230.03 | 354.75 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 272.82 | 275.14 | 275.05 | 272.85 | 277.63 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 225.80 | 225.72 | 225.91 | 224.85 | 226.23 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 286.00 | 277.78 | 279.37 | 271.87 | 280.49 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 443.38 | 393.29 | 392.98 | 390.43 | 396.76 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 1007.92 | 940.36 | 909.83 | 901.74 | 1040.04 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 442.84 | 416.11 | 416.26 | 413.28 | 418.62 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 317.18 | 236.31 | 236.19 | 232.75 | 240.09 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 279.60 | 258.98 | 258.87 | 257.44 | 260.74 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 983.80 | 802.81 | 793.38 | 762.23 | 862.25 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 394.73 | 454.56 | 422.04 | 381.25 | 592.92 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 446.06 | 425.62 | 425.24 | 416.56 | 435.42 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 680.77 | 678.73 | 678.43 | 675.65 | 682.40 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 368.15 | 335.57 | 333.70 | 333.17 | 341.71 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 246.63 | 243.33 | 243.18 | 242.11 | 244.84 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 83.46 | 67.42 | 67.47 | 66.89 | 67.87 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 546.56 | 528.42 | 455.30 | 430.32 | 772.78 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 114.91 | 94.44 | 94.34 | 93.91 | 95.14 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 51.04 | 32.34 | 32.35 | 31.78 | 32.86 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 569.53 | 569.55 | 569.60 | 565.12 | 573.88 | ok |
<!-- benchmark-table:end -->
