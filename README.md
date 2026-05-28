# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-28 15:08:08` · `queries 43/43` · `warm runs 4` · [`csv`](benchmarks/results/readme_benchmarks.csv)

<table>
<tr>
<td valign="top" width="50%">
<h3>Summary</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>queries ok</td><td align="right">43 / 43</td></tr>
<tr><td>queries failed</td><td align="right">0</td></tr>
<tr><td>median warm time</td><td align="right">229.34 ms</td></tr>
<tr><td>average warm time</td><td align="right">230.90 ms</td></tr>
<tr><td>p95 warm time</td><td align="right">446.96 ms</td></tr>
<tr><td>cold/warm delta</td><td align="right">6.3%</td></tr>
<tr><td>total output size</td><td align="right">20.3 KB</td></tr>
<tr><td>max output size</td><td align="right">6.0 KB</td></tr>
<tr><td>fastest query</td><td align="right">Q06 · 4.26 ms</td></tr>
<tr><td>slowest query</td><td align="right">Q28 · 979.97 ms</td></tr>
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
<tr><td>csv -&gt; columnar</td><td align="right">5.67s</td></tr>
<tr><td>columnar -&gt; csv</td><td align="right">9.59s</td></tr>
<tr><td>convert throughput</td><td align="right">141.7 MB/s</td></tr>
<tr><td>roundtrip throughput</td><td align="right">12.5 MB/s</td></tr>
</tbody>
</table>
</td>
</tr>
</table>

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms

| slot 1 | slot 2 | slot 3 | slot 4 | slot 5 | slot 6 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| Q00 🟩 `4.87` | Q01 🟩 `7.24` | Q02 🟩 `18.79` | Q03 🟩 `14.52` | Q04 🟩 `167.49` | Q05 🟩 `132.28` |
| Q06 🟩 `4.26` | Q07 🟩 `7.09` | Q08 🟩 `204.97` | Q09 🟦 `217.93` | Q10 🟥 `268.97` | Q11 🟥 `269.99` |
| Q12 🟦 `229.34` | Q13 🟦 `246.09` | Q14 🟦 `232.91` | Q15 🟩 `75.99` | Q16 🟩 `129.28` | Q17 🟩 `152.80` |
| Q18 🟥 `374.64` | Q19 🟩 `5.12` | Q20 🟩 `145.69` | Q21 🟥 `277.66` | Q22 🟥 `374.66` | Q23 🟦 `244.56` |
| Q24 🟥 `268.40` | Q25 🟦 `219.57` | Q26 🟥 `270.06` | Q27 🟥 `420.86` | Q28 🟥 `979.97` | Q29 🟥 `391.93` |
| Q30 🟦 `237.88` | Q31 🟥 `266.14` | Q32 🟥 `776.24` | Q33 🟥 `405.27` | Q34 🟥 `422.68` | Q35 🟩 `155.09` |
| Q36 🟥 `342.61` | Q37 🟦 `243.88` | Q38 🟩 `74.44` | Q39 🟥 `449.66` | Q40 🟩 `93.72` | Q41 🟩 `34.09` |
| Q42 🟩 `69.12` |  |  |  |  |  |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 9.49 | 4.96 | 4.87 | 4.59 | 5.51 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 15.88 | 7.12 | 7.24 | 6.19 | 7.81 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 28.03 | 19.08 | 18.79 | 17.92 | 20.83 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 27.65 | 14.48 | 14.52 | 14.06 | 14.81 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 156.02 | 190.53 | 167.49 | 153.12 | 274.03 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 142.67 | 132.55 | 132.28 | 132.05 | 133.58 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 4.78 | 4.27 | 4.26 | 4.12 | 4.43 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 7.78 | 7.08 | 7.09 | 6.68 | 7.45 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 204.57 | 205.39 | 204.97 | 201.88 | 209.73 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 217.28 | 218.49 | 217.93 | 215.12 | 222.97 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 281.97 | 269.05 | 268.97 | 268.52 | 269.74 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 273.22 | 270.25 | 269.99 | 269.17 | 271.86 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 227.97 | 230.01 | 229.34 | 227.64 | 233.73 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 249.96 | 246.34 | 246.09 | 244.41 | 248.77 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 232.70 | 233.59 | 232.91 | 231.02 | 237.51 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 76.95 | 75.84 | 75.99 | 74.37 | 77.04 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 130.83 | 129.57 | 129.28 | 129.09 | 130.63 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 153.61 | 153.56 | 152.80 | 152.21 | 156.42 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 445.38 | 374.33 | 374.64 | 372.72 | 375.32 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 5.02 | 5.16 | 5.12 | 4.54 | 5.86 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 194.65 | 145.74 | 145.69 | 145.27 | 146.31 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 278.26 | 277.79 | 277.66 | 277.15 | 278.69 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 426.01 | 375.71 | 374.66 | 374.01 | 379.49 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 285.01 | 244.43 | 244.56 | 242.75 | 245.86 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 658 B | 268.59 | 268.79 | 268.40 | 267.75 | 270.62 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 222.70 | 219.63 | 219.57 | 219.47 | 219.92 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 268.76 | 270.05 | 270.06 | 269.16 | 270.92 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 425.51 | 420.92 | 420.86 | 416.18 | 425.80 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 979.03 | 1016.62 | 979.97 | 974.72 | 1131.81 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 395.85 | 393.43 | 391.93 | 390.74 | 399.12 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 250.48 | 238.09 | 237.88 | 236.56 | 240.02 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 280.27 | 266.13 | 266.14 | 265.27 | 266.99 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 1030.95 | 777.39 | 776.24 | 769.61 | 787.48 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 403.13 | 404.75 | 405.27 | 402.76 | 405.69 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 453.77 | 422.89 | 422.68 | 419.71 | 426.50 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 156.60 | 155.02 | 155.09 | 153.26 | 156.64 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 347.88 | 342.61 | 342.61 | 342.31 | 342.92 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 244.48 | 243.77 | 243.88 | 242.73 | 244.58 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 80.37 | 74.58 | 74.44 | 74.34 | 75.09 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 453.18 | 450.37 | 449.66 | 447.42 | 454.74 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 111.18 | 93.89 | 93.72 | 93.59 | 94.54 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 37.92 | 34.08 | 34.09 | 33.77 | 34.37 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 68.61 | 69.07 | 69.12 | 68.65 | 69.40 | ok |
<!-- benchmark-table:end -->
