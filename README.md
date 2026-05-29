# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-29 18:28:35` · `queries 43/43` · `warm runs 4` · `cache drop per-query` · [`csv`](benchmarks/results/readme_benchmarks.csv)

<table>
<tr>
<td valign="top" width="50%">
<h3>Summary</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>queries ok</td><td align="right">43 / 43</td></tr>
<tr><td>queries failed</td><td align="right">0</td></tr>
<tr><td>median warm time</td><td align="right">228.91 ms</td></tr>
<tr><td>average warm time</td><td align="right">250.97 ms</td></tr>
<tr><td>p95 warm time</td><td align="right">484.49 ms</td></tr>
<tr><td>cold/warm delta</td><td align="right">69.2%</td></tr>
<tr><td>total output size</td><td align="right">20.3 KB</td></tr>
<tr><td>max output size</td><td align="right">6.0 KB</td></tr>
<tr><td>fastest query</td><td align="right">Q00 · 4.32 ms</td></tr>
<tr><td>slowest query</td><td align="right">Q28 · 1037.26 ms</td></tr>
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
<tr><td>csv -&gt; columnar</td><td align="right">6.15s</td></tr>
<tr><td>columnar -&gt; csv</td><td align="right">9.81s</td></tr>
<tr><td>convert throughput</td><td align="right">130.5 MB/s</td></tr>
<tr><td>roundtrip throughput</td><td align="right">12.2 MB/s</td></tr>
</tbody>
</table>
</td>
</tr>
</table>

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 🟩 `4.32` | Q01 🟩 `11.76` | Q02 🟩 `22.01` | Q03 🟩 `18.75` | Q04 🟩 `149.04` | Q05 🟩 `130.82` |
| Q06 🟩 `4.89` | Q07 🟩 `34.49` | Q08 🟦 `209.66` | Q09 🟦 `210.86` | Q10 🟥 `321.56` | Q11 🟥 `269.94` |
| Q12 🟦 `225.75` | Q13 🟥 `254.04` | Q14 🟦 `229.45` | Q15 🟩 `77.77` | Q16 🟩 `123.55` | Q17 🟩 `155.37` |
| Q18 🟥 `409.06` | Q19 🟩 `4.78` | Q20 🟦 `250.25` | Q21 🟥 `274.39` | Q22 🟥 `379.26` | Q23 🟥 `259.48` |
| Q24 🟦 `244.56` | Q25 🟦 `228.91` | Q26 🟦 `225.43` | Q27 🟥 `484.59` | Q28 🟥 `1037.26` | Q29 🟥 `483.60` |
| Q30 🟦 `247.41` | Q31 🟥 `289.23` | Q32 🟥 `970.94` | Q33 🟥 `435.10` | Q34 🟥 `467.90` | Q35 🟩 `185.10` |
| Q36 🟥 `470.23` | Q37 🟥 `272.08` | Q38 🟩 `72.51` | Q39 🟥 `447.60` | Q40 🟩 `93.99` | Q41 🟩 `34.60` |
| Q42 🟩 `69.50` |  |  |  |  |  |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 113.25 | 4.35 | 4.32 | 4.12 | 4.63 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 137.27 | 21.96 | 11.76 | 11.52 | 52.82 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 142.08 | 22.02 | 22.01 | 21.74 | 22.31 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 123.64 | 18.46 | 18.75 | 16.21 | 20.13 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 234.96 | 150.05 | 149.04 | 148.08 | 154.04 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 240.36 | 130.98 | 130.82 | 129.98 | 132.32 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 85.34 | 5.07 | 4.89 | 4.46 | 6.01 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 239.83 | 31.86 | 34.49 | 14.17 | 44.29 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 580.94 | 209.67 | 209.66 | 195.59 | 223.77 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 375.45 | 212.19 | 210.86 | 210.84 | 216.20 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 398.40 | 330.03 | 321.56 | 268.15 | 408.83 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 406.75 | 270.12 | 269.94 | 268.30 | 272.31 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 358.79 | 226.49 | 225.75 | 224.20 | 230.24 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 418.79 | 254.61 | 254.04 | 252.88 | 257.49 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 432.13 | 229.34 | 229.45 | 226.48 | 231.97 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 202.02 | 77.58 | 77.77 | 75.24 | 79.54 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 328.95 | 123.58 | 123.55 | 122.32 | 124.89 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 314.07 | 156.98 | 155.37 | 154.82 | 162.36 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 593.68 | 397.69 | 409.06 | 359.36 | 413.31 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 136.87 | 4.81 | 4.78 | 4.61 | 5.08 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 429.77 | 250.00 | 250.25 | 248.39 | 251.10 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 501.89 | 275.00 | 274.39 | 272.76 | 278.44 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 613.76 | 388.76 | 379.26 | 374.02 | 422.49 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 535.96 | 256.21 | 259.48 | 244.46 | 261.44 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 672 B | 481.76 | 244.80 | 244.56 | 238.24 | 251.85 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 323.60 | 237.19 | 228.91 | 221.17 | 269.75 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 524.01 | 225.42 | 225.43 | 223.30 | 227.52 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 630.87 | 538.84 | 484.59 | 473.97 | 712.22 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 1281.69 | 1032.44 | 1037.26 | 974.01 | 1081.23 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 688.07 | 492.24 | 483.60 | 456.93 | 544.81 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 417.32 | 247.59 | 247.41 | 242.87 | 252.69 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 537.23 | 288.86 | 289.23 | 274.97 | 302.04 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 1146.97 | 972.70 | 970.94 | 952.74 | 996.17 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 565.29 | 445.48 | 435.10 | 373.51 | 538.19 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 601.70 | 470.54 | 467.90 | 440.92 | 505.46 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 377.26 | 189.25 | 185.10 | 172.52 | 214.30 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 505.12 | 595.08 | 470.23 | 367.62 | 1072.25 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 652.50 | 269.23 | 272.08 | 246.11 | 286.66 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 221.58 | 75.18 | 72.51 | 71.88 | 83.83 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 620.65 | 435.00 | 447.60 | 393.28 | 451.52 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 275.34 | 93.89 | 93.99 | 93.48 | 94.10 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 246.34 | 34.89 | 34.60 | 33.65 | 36.68 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 217.42 | 69.30 | 69.50 | 67.93 | 70.24 | ok |
<!-- benchmark-table:end -->
