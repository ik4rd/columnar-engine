# columnar-engine

Колононый движок с нуля (in progress)

## Среда выполнения benchmark'ов

- Устройство: MacBook Pro (`MacBookPro17,1`)
- Процессор: Apple M1
- Ядра CPU: 8 (`4` performance + `4` efficiency)
- Оперативная память: 8 GB
- Архитектура: `arm64`
- ОС: macOS `26.3` (`25D5101c`)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-06-04 15:18:18` · `queries 43/43` · `warm runs 4` · `cache drop per-query` · `query mode hardcoded` · [`csv`](benchmarks/results/readme_benchmarks.csv)

<table>
<tr>
<td valign="top" width="50%">
<h3>Summary</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>queries ok</td><td align="right">43 / 43</td></tr>
<tr><td>queries failed</td><td align="right">0</td></tr>
<tr><td>median warm time</td><td align="right">110.75 ms</td></tr>
<tr><td>average warm time</td><td align="right">155.90 ms</td></tr>
<tr><td>p95 warm time</td><td align="right">386.32 ms</td></tr>
<tr><td>cold/warm delta</td><td align="right">23.1%</td></tr>
<tr><td>total output size</td><td align="right">0 B</td></tr>
<tr><td>max output size</td><td align="right">0 B</td></tr>
<tr><td>fastest query</td><td align="right">Q00 · &lt;1 ms</td></tr>
<tr><td>slowest query</td><td align="right">Q28 · 899.50 ms</td></tr>
</tbody>
</table>
</td>
<td valign="top" width="50%">
<h3>Storage</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>source csv</td><td align="right">benchmarks/hits_sample.csv</td></tr>
<tr><td>schema</td><td align="right">benchmarks/scheme.csv</td></tr>
<tr><td>compression</td><td align="right">lz4</td></tr>
<tr><td>source size</td><td align="right">802.6 MB</td></tr>
<tr><td>columnar size</td><td align="right">119.7 MB</td></tr>
<tr><td>roundtrip csv size</td><td align="right">743.0 MB</td></tr>
<tr><td>compression ratio</td><td align="right">6.71x</td></tr>
<tr><td>columnar / csv</td><td align="right">14.9%</td></tr>
<tr><td>csv -&gt; columnar</td><td align="right">6.33s</td></tr>
<tr><td>columnar -&gt; csv</td><td align="right">9.23s</td></tr>
<tr><td>convert throughput</td><td align="right">126.8 MB/s</td></tr>
<tr><td>roundtrip throughput</td><td align="right">13.0 MB/s</td></tr>
</tbody>
</table>
</td>
</tr>
</table>

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` <1 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 ⬜ `<1` | Q01 🟩 `4.00` | Q02 🟩 `7.00` | Q03 🟩 `5.00` | Q04 🟩 `37.00` | Q05 🟩 `25.50` |
| Q06 ⬜ `<1` | Q07 🟩 `7.00` | Q08 🟥 `184.00` | Q09 🟥 `186.00` | Q10 🟥 `253.50` | Q11 🟥 `262.50` |
| Q12 🟥 `205.00` | Q13 🟥 `235.00` | Q14 🟥 `258.00` | Q15 🟩 `57.00` | Q16 🟦 `113.00` | Q17 🟥 `140.00` |
| Q18 🟥 `360.00` | Q19 ⬜ `<1` | Q20 🟩 `98.50` | Q21 🟦 `108.50` | Q22 🟥 `180.00` | Q23 🟥 `180.00` |
| Q24 🟩 `16.00` | Q25 🟩 `12.00` | Q26 🟩 `16.00` | Q27 🟥 `402.00` | Q28 🟥 `899.50` | Q29 🟩 `4.00` |
| Q30 🟩 `24.50` | Q31 🟩 `38.00` | Q32 🟥 `385.50` | Q33 🟥 `194.00` | Q34 🟥 `230.50` | Q35 🟩 `31.50` |
| Q36 🟥 `320.50` | Q37 🟥 `220.00` | Q38 🟩 `47.00` | Q39 🟥 `348.00` | Q40 🟩 `42.00` | Q41 🟩 `29.50` |
| Q42 🟩 `69.00` |  |  |  |  |  |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 0 B | <1 | <1 | <1 | <1 | <1 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 0 B | 13.00 | 4.00 | 4.00 | 4.00 | 4.00 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 0 B | 27.00 | 6.75 | 7.00 | 6.00 | 7.00 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 0 B | 21.00 | 5.00 | 5.00 | 5.00 | 5.00 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 0 B | 60.00 | 55.50 | 37.00 | 35.00 | 113.00 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 0 B | 46.00 | 25.50 | 25.50 | 25.00 | 26.00 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 0 B | <1 | <1 | <1 | <1 | <1 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 0 B | 16.00 | 7.00 | 7.00 | 7.00 | 7.00 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 0 B | 219.00 | 185.50 | 184.00 | 176.00 | 198.00 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 0 B | 235.00 | 186.25 | 186.00 | 185.00 | 188.00 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 0 B | 287.00 | 253.75 | 253.50 | 253.00 | 255.00 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 0 B | 295.00 | 262.75 | 262.50 | 259.00 | 267.00 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 0 B | 228.00 | 204.75 | 205.00 | 204.00 | 205.00 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 0 B | 273.00 | 235.00 | 235.00 | 234.00 | 236.00 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 0 B | 240.00 | 257.25 | 258.00 | 225.00 | 288.00 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 0 B | 86.00 | 57.25 | 57.00 | 57.00 | 58.00 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 0 B | 143.00 | 137.25 | 113.00 | 94.00 | 229.00 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 0 B | 183.00 | 141.50 | 140.00 | 134.00 | 152.00 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 0 B | 419.00 | 363.75 | 360.00 | 353.00 | 382.00 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 2.00 | <1 | <1 | <1 | <1 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 0 B | 156.00 | 98.50 | 98.50 | 97.00 | 100.00 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 0 B | 175.00 | 108.25 | 108.50 | 107.00 | 109.00 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 0 B | 269.00 | 186.25 | 180.00 | 179.00 | 206.00 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 0 B | 282.00 | 185.50 | 180.00 | 157.00 | 225.00 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 0 B | 79.00 | 16.25 | 16.00 | 16.00 | 17.00 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 0 B | 33.00 | 12.25 | 12.00 | 12.00 | 13.00 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 0 B | 74.00 | 16.00 | 16.00 | 16.00 | 16.00 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 0 B | 449.00 | 404.00 | 402.00 | 393.00 | 419.00 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 0 B | 1239.00 | 905.50 | 899.50 | 853.00 | 970.00 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 0 B | 15.00 | 4.00 | 4.00 | 4.00 | 4.00 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 0 B | 77.00 | 24.50 | 24.50 | 24.00 | 25.00 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 0 B | 137.00 | 38.50 | 38.00 | 37.00 | 41.00 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 0 B | 481.00 | 390.25 | 385.50 | 378.00 | 412.00 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 0 B | 256.00 | 196.75 | 194.00 | 191.00 | 208.00 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 0 B | 381.00 | 274.25 | 230.50 | 204.00 | 432.00 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 0 B | 38.00 | 35.75 | 31.50 | 21.00 | 59.00 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 0 B | 347.00 | 324.75 | 320.50 | 315.00 | 343.00 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 0 B | 245.00 | 237.25 | 220.00 | 209.00 | 300.00 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 0 B | 97.00 | 47.25 | 47.00 | 47.00 | 48.00 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 0 B | 396.00 | 348.00 | 348.00 | 346.00 | 350.00 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 0 B | 72.00 | 42.00 | 42.00 | 41.00 | 43.00 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 0 B | 62.00 | 29.25 | 29.50 | 28.00 | 30.00 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 0 B | 98.00 | 69.75 | 69.00 | 68.00 | 73.00 | ok |
<!-- benchmark-table:end -->
