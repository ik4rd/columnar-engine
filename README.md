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

`generated 2026-05-31 15:54:25` · `queries 43/43` · `warm runs 4` · `cache drop none` · `query mode hardcoded` · [`csv`](benchmarks/results/readme_benchmarks.csv)

<table>
<tr>
<td valign="top" width="50%">
<h3>Summary</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>queries ok</td><td align="right">43 / 43</td></tr>
<tr><td>queries failed</td><td align="right">0</td></tr>
<tr><td>median warm time</td><td align="right">152.25 ms</td></tr>
<tr><td>average warm time</td><td align="right">194.25 ms</td></tr>
<tr><td>p95 warm time</td><td align="right">518.62 ms</td></tr>
<tr><td>cold/warm delta</td><td align="right">-8.4%</td></tr>
<tr><td>total output size</td><td align="right">0 B</td></tr>
<tr><td>max output size</td><td align="right">0 B</td></tr>
<tr><td>fastest query</td><td align="right">Q00 · &lt;1 ms</td></tr>
<tr><td>slowest query</td><td align="right">Q28 · 919.50 ms</td></tr>
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
<tr><td>csv -&gt; columnar</td><td align="right">5.69s</td></tr>
<tr><td>columnar -&gt; csv</td><td align="right">9.57s</td></tr>
<tr><td>convert throughput</td><td align="right">141.0 MB/s</td></tr>
<tr><td>roundtrip throughput</td><td align="right">12.5 MB/s</td></tr>
</tbody>
</table>
</td>
</tr>
</table>

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` <1 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 ⬜ `<1` | Q01 🟩 `4.00` | Q02 🟩 `8.50` | Q03 🟩 `6.00` | Q04 🟩 `34.50` | Q05 🟩 `37.50` |
| Q06 ⬜ `<1` | Q07 🟩 `7.00` | Q08 🟥 `191.00` | Q09 🟥 `214.00` | Q10 🟥 `272.00` | Q11 🟥 `267.50` |
| Q12 🟥 `225.50` | Q13 🟥 `259.00` | Q14 🟥 `227.00` | Q15 🟩 `99.00` | Q16 🟦 `149.50` | Q17 🟦 `162.50` |
| Q18 🟥 `510.00` | Q19 ⬜ `<1` | Q20 🟦 `137.50` | Q21 🟦 `155.00` | Q22 🟥 `286.00` | Q23 🟥 `245.00` |
| Q24 🟩 `39.00` | Q25 🟩 `35.00` | Q26 🟩 `38.00` | Q27 🟥 `453.00` | Q28 🟥 `919.50` | Q29 🟩 `7.50` |
| Q30 🟩 `38.00` | Q31 🟩 `56.00` | Q32 🟥 `682.50` | Q33 🟥 `347.50` | Q34 🟥 `333.50` | Q35 🟩 `20.50` |
| Q36 🟥 `369.00` | Q37 🟥 `240.00` | Q38 🟩 `70.50` | Q39 🟥 `461.00` | Q40 🟩 `64.50` | Q41 🟩 `32.50` |
| Q42 🟩 `64.50` |  |  |  |  |  |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 0 B | <1 | <1 | <1 | <1 | <1 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 0 B | 9.00 | 4.25 | 4.00 | 4.00 | 5.00 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 0 B | 11.00 | 8.50 | 8.50 | 7.00 | 10.00 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 0 B | 12.00 | 6.00 | 6.00 | 6.00 | 6.00 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 0 B | 33.00 | 34.75 | 34.50 | 34.00 | 36.00 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 0 B | 44.00 | 37.50 | 37.50 | 36.00 | 39.00 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 0 B | <1 | <1 | <1 | <1 | <1 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 0 B | 8.00 | 7.00 | 7.00 | 7.00 | 7.00 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 0 B | 189.00 | 190.75 | 191.00 | 188.00 | 193.00 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 0 B | 202.00 | 225.00 | 214.00 | 209.00 | 263.00 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 0 B | 267.00 | 272.50 | 272.00 | 268.00 | 278.00 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 0 B | 264.00 | 268.50 | 267.50 | 266.00 | 273.00 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 0 B | 222.00 | 226.25 | 225.50 | 223.00 | 231.00 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 0 B | 253.00 | 258.25 | 259.00 | 252.00 | 263.00 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 0 B | 222.00 | 226.50 | 227.00 | 222.00 | 230.00 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 0 B | 90.00 | 98.75 | 99.00 | 91.00 | 106.00 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 0 B | 140.00 | 151.50 | 149.50 | 144.00 | 163.00 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 0 B | 151.00 | 162.75 | 162.50 | 158.00 | 168.00 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 0 B | 512.00 | 511.25 | 510.00 | 504.00 | 521.00 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | <1 | 0.25 | <1 | <1 | 1.00 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 0 B | 156.00 | 137.25 | 137.50 | 136.00 | 138.00 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 0 B | 152.00 | 155.50 | 155.00 | 154.00 | 158.00 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 0 B | 312.00 | 285.50 | 286.00 | 282.00 | 288.00 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 0 B | 258.00 | 245.00 | 245.00 | 238.00 | 252.00 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 0 B | 36.00 | 38.25 | 39.00 | 36.00 | 39.00 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 0 B | 35.00 | 36.25 | 35.00 | 35.00 | 40.00 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 0 B | 37.00 | 41.25 | 38.00 | 37.00 | 52.00 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 0 B | 441.00 | 452.00 | 453.00 | 441.00 | 461.00 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 0 B | 961.00 | 919.25 | 919.50 | 915.00 | 923.00 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 0 B | 5.00 | 7.00 | 7.50 | 4.00 | 9.00 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 0 B | 40.00 | 37.75 | 38.00 | 36.00 | 39.00 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 0 B | 50.00 | 55.00 | 56.00 | 50.00 | 58.00 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 0 B | 626.00 | 820.25 | 682.50 | 668.00 | 1248.00 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 0 B | 314.00 | 351.75 | 347.50 | 340.00 | 372.00 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 0 B | 302.00 | 333.25 | 333.50 | 328.00 | 338.00 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 0 B | 20.00 | 21.25 | 20.50 | 19.00 | 25.00 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 0 B | 351.00 | 379.75 | 369.00 | 363.00 | 418.00 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 0 B | 233.00 | 245.00 | 240.00 | 240.00 | 260.00 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 0 B | 68.00 | 71.75 | 70.50 | 69.00 | 77.00 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 0 B | 460.00 | 475.25 | 461.00 | 459.00 | 520.00 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 0 B | 72.00 | 64.25 | 64.50 | 60.00 | 68.00 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 0 B | 31.00 | 32.50 | 32.50 | 30.00 | 35.00 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 0 B | 64.00 | 65.00 | 64.50 | 64.00 | 67.00 | ok |
<!-- benchmark-table:end -->
