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

`generated 2026-05-31 17:18:21` · `queries 43/43` · `warm runs 4` · `cache drop per-query` · `query mode hardcoded` · [`csv`](benchmarks/results/readme_benchmarks.csv)

<table>
<tr>
<td valign="top" width="50%">
<h3>Summary</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>queries ok</td><td align="right">43 / 43</td></tr>
<tr><td>queries failed</td><td align="right">0</td></tr>
<tr><td>median warm time</td><td align="right">149.50 ms</td></tr>
<tr><td>average warm time</td><td align="right">190.25 ms</td></tr>
<tr><td>p95 warm time</td><td align="right">521.35 ms</td></tr>
<tr><td>cold/warm delta</td><td align="right">17.8%</td></tr>
<tr><td>total output size</td><td align="right">0 B</td></tr>
<tr><td>max output size</td><td align="right">0 B</td></tr>
<tr><td>fastest query</td><td align="right">Q00 · &lt;1 ms</td></tr>
<tr><td>slowest query</td><td align="right">Q28 · 940.00 ms</td></tr>
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
<tr><td>csv -&gt; columnar</td><td align="right">5.71s</td></tr>
<tr><td>columnar -&gt; csv</td><td align="right">9.46s</td></tr>
<tr><td>convert throughput</td><td align="right">140.5 MB/s</td></tr>
<tr><td>roundtrip throughput</td><td align="right">12.6 MB/s</td></tr>
</tbody>
</table>
</td>
</tr>
</table>

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` <1 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 ⬜ `<1` | Q01 🟩 `4.00` | Q02 🟩 `7.00` | Q03 🟩 `6.00` | Q04 🟩 `34.50` | Q05 🟩 `36.50` |
| Q06 ⬜ `<1` | Q07 🟩 `8.00` | Q08 🟥 `189.50` | Q09 🟥 `205.00` | Q10 🟥 `260.50` | Q11 🟥 `271.00` |
| Q12 🟥 `223.00` | Q13 🟥 `252.50` | Q14 🟥 `226.00` | Q15 🟩 `89.50` | Q16 🟦 `143.00` | Q17 🟦 `157.00` |
| Q18 🟥 `516.00` | Q19 ⬜ `<1` | Q20 🟦 `136.50` | Q21 🟦 `156.00` | Q22 🟥 `280.00` | Q23 🟥 `239.00` |
| Q24 🟩 `39.00` | Q25 🟩 `36.50` | Q26 🟩 `39.50` | Q27 🟥 `450.50` | Q28 🟥 `940.00` | Q29 🟩 `5.00` |
| Q30 🟩 `37.50` | Q31 🟩 `52.50` | Q32 🟥 `623.00` | Q33 🟥 `322.00` | Q34 🟥 `323.00` | Q35 🟩 `20.00` |
| Q36 🟥 `358.50` | Q37 🟥 `242.50` | Q38 🟩 `70.50` | Q39 🟥 `462.00` | Q40 🟩 `53.00` | Q41 🟩 `30.00` |
| Q42 🟩 `64.50` |  |  |  |  |  |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 0 B | <1 | <1 | <1 | <1 | <1 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 0 B | 16.00 | 4.00 | 4.00 | 4.00 | 4.00 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 0 B | 31.00 | 7.00 | 7.00 | 7.00 | 7.00 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 0 B | 24.00 | 6.00 | 6.00 | 6.00 | 6.00 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 0 B | 52.00 | 34.50 | 34.50 | 34.00 | 35.00 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 0 B | 65.00 | 36.75 | 36.50 | 36.00 | 38.00 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 0 B | <1 | <1 | <1 | <1 | <1 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 0 B | 18.00 | 7.75 | 8.00 | 7.00 | 8.00 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 0 B | 213.00 | 189.25 | 189.50 | 186.00 | 192.00 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 0 B | 257.00 | 205.25 | 205.00 | 204.00 | 207.00 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 0 B | 296.00 | 262.25 | 260.50 | 260.00 | 268.00 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 0 B | 306.00 | 269.50 | 271.00 | 262.00 | 274.00 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 0 B | 260.00 | 224.25 | 223.00 | 221.00 | 230.00 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 0 B | 298.00 | 253.00 | 252.50 | 251.00 | 256.00 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 0 B | 257.00 | 226.00 | 226.00 | 225.00 | 227.00 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 0 B | 108.00 | 89.75 | 89.50 | 88.00 | 92.00 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 0 B | 193.00 | 143.00 | 143.00 | 142.00 | 144.00 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 0 B | 216.00 | 157.75 | 157.00 | 154.00 | 163.00 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 0 B | 589.00 | 517.50 | 516.00 | 509.00 | 529.00 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 3.00 | <1 | <1 | <1 | <1 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 0 B | 202.00 | 136.25 | 136.50 | 135.00 | 137.00 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 0 B | 245.00 | 156.75 | 156.00 | 156.00 | 159.00 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 0 B | 383.00 | 284.50 | 280.00 | 272.00 | 306.00 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 0 B | 365.00 | 241.25 | 239.00 | 238.00 | 249.00 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 0 B | 101.00 | 38.75 | 39.00 | 38.00 | 39.00 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 0 B | 65.00 | 36.50 | 36.50 | 36.00 | 37.00 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 0 B | 103.00 | 39.75 | 39.50 | 39.00 | 41.00 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 0 B | 534.00 | 454.75 | 450.50 | 447.00 | 471.00 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 0 B | 993.00 | 940.75 | 940.00 | 934.00 | 949.00 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 0 B | 17.00 | 4.75 | 5.00 | 4.00 | 5.00 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 0 B | 99.00 | 37.75 | 37.50 | 37.00 | 39.00 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 0 B | 161.00 | 53.50 | 52.50 | 51.00 | 58.00 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 0 B | 744.00 | 633.25 | 623.00 | 614.00 | 673.00 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 0 B | 381.00 | 323.00 | 322.00 | 319.00 | 329.00 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 0 B | 386.00 | 323.75 | 323.00 | 319.00 | 330.00 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 0 B | 43.00 | 20.00 | 20.00 | 20.00 | 20.00 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 0 B | 414.00 | 358.25 | 358.50 | 354.00 | 362.00 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 0 B | 284.00 | 242.25 | 242.50 | 240.00 | 244.00 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 0 B | 118.00 | 70.25 | 70.50 | 68.00 | 72.00 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 0 B | 528.00 | 461.00 | 462.00 | 455.00 | 465.00 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 0 B | 105.00 | 53.00 | 53.00 | 52.00 | 54.00 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 0 B | 70.00 | 30.00 | 30.00 | 30.00 | 30.00 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 0 B | 95.00 | 64.50 | 64.50 | 64.00 | 65.00 | ok |
<!-- benchmark-table:end -->
