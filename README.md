# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Benchmark Dashboard

`generated 2026-05-28 21:28:51` · `queries 43/43` · `warm runs 4` · [`csv`](benchmarks/results/readme_benchmarks.csv)

<table>
<tr>
<td valign="top" width="50%">
<h3>Summary</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>queries ok</td><td align="right">43 / 43</td></tr>
<tr><td>queries failed</td><td align="right">0</td></tr>
<tr><td>median warm time</td><td align="right">220.81 ms</td></tr>
<tr><td>average warm time</td><td align="right">231.63 ms</td></tr>
<tr><td>p95 warm time</td><td align="right">476.62 ms</td></tr>
<tr><td>cold/warm delta</td><td align="right">5.3%</td></tr>
<tr><td>total output size</td><td align="right">20.3 KB</td></tr>
<tr><td>max output size</td><td align="right">6.0 KB</td></tr>
<tr><td>fastest query</td><td align="right">Q06 · 4.12 ms</td></tr>
<tr><td>slowest query</td><td align="right">Q28 · 969.54 ms</td></tr>
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
<tr><td>csv -&gt; columnar</td><td align="right">5.81s</td></tr>
<tr><td>columnar -&gt; csv</td><td align="right">9.84s</td></tr>
<tr><td>convert throughput</td><td align="right">138.1 MB/s</td></tr>
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
| Q00 🟩 `7.14` | Q01 🟩 `12.85` | Q02 🟩 `21.68` | Q03 🟩 `14.30` | Q04 🟩 `147.02` | Q05 🟩 `128.76` |
| Q06 🟩 `4.12` | Q07 🟩 `11.69` | Q08 🟩 `189.51` | Q09 🟦 `208.53` | Q10 🟥 `267.84` | Q11 🟥 `267.06` |
| Q12 🟦 `225.37` | Q13 🟥 `253.71` | Q14 🟦 `227.55` | Q15 🟩 `74.16` | Q16 🟩 `116.97` | Q17 🟩 `146.00` |
| Q18 🟥 `349.51` | Q19 🟩 `4.39` | Q20 🟥 `252.69` | Q21 🟥 `277.31` | Q22 🟥 `364.71` | Q23 🟦 `240.62` |
| Q24 🟦 `220.81` | Q25 🟦 `219.69` | Q26 🟦 `220.03` | Q27 🟥 `450.51` | Q28 🟥 `969.54` | Q29 🟥 `427.54` |
| Q30 🟦 `235.10` | Q31 🟥 `262.43` | Q32 🟥 `881.20` | Q33 🟥 `349.73` | Q34 🟥 `420.41` | Q35 🟩 `149.57` |
| Q36 🟥 `339.52` | Q37 🟥 `247.46` | Q38 🟩 `73.93` | Q39 🟥 `479.52` | Q40 🟩 `95.10` | Q41 🟩 `34.88` |
| Q42 🟩 `69.69` |  |  |  |  |  |

### Query Table

| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| [Q00](benchmarks/queries/query_0.sql) | 7 B | 372.47 | 7.12 | 7.14 | 5.32 | 8.89 | ok |
| [Q01](benchmarks/queries/query_1.sql) | 6 B | 16.18 | 13.17 | 12.85 | 11.37 | 15.61 | ok |
| [Q02](benchmarks/queries/query_2.sql) | 18 B | 28.44 | 21.85 | 21.68 | 21.58 | 22.45 | ok |
| [Q03](benchmarks/queries/query_3.sql) | 20 B | 14.56 | 14.35 | 14.30 | 14.24 | 14.55 | ok |
| [Q04](benchmarks/queries/query_4.sql) | 6 B | 151.94 | 148.55 | 147.02 | 146.46 | 153.70 | ok |
| [Q05](benchmarks/queries/query_5.sql) | 6 B | 131.23 | 128.78 | 128.76 | 128.60 | 129.00 | ok |
| [Q06](benchmarks/queries/query_6.sql) | 22 B | 4.63 | 4.11 | 4.12 | 4.00 | 4.22 | ok |
| [Q07](benchmarks/queries/query_7.sql) | 31 B | 11.54 | 11.68 | 11.69 | 11.61 | 11.71 | ok |
| [Q08](benchmarks/queries/query_8.sql) | 81 B | 189.62 | 189.70 | 189.51 | 189.29 | 190.49 | ok |
| [Q09](benchmarks/queries/query_9.sql) | 237 B | 209.27 | 209.04 | 208.53 | 207.81 | 211.30 | ok |
| [Q10](benchmarks/queries/query_10.sql) | 102 B | 273.63 | 268.25 | 267.84 | 264.68 | 272.65 | ok |
| [Q11](benchmarks/queries/query_11.sql) | 116 B | 267.74 | 267.06 | 267.06 | 266.30 | 267.81 | ok |
| [Q12](benchmarks/queries/query_12.sql) | 387 B | 225.38 | 225.50 | 225.37 | 225.15 | 226.12 | ok |
| [Q13](benchmarks/queries/query_13.sql) | 350 B | 255.52 | 253.82 | 253.71 | 253.17 | 254.72 | ok |
| [Q14](benchmarks/queries/query_14.sql) | 400 B | 227.71 | 227.58 | 227.55 | 227.02 | 228.18 | ok |
| [Q15](benchmarks/queries/query_15.sql) | 236 B | 73.92 | 74.08 | 74.16 | 73.23 | 74.78 | ok |
| [Q16](benchmarks/queries/query_16.sql) | 244 B | 116.27 | 117.15 | 116.97 | 116.74 | 117.90 | ok |
| [Q17](benchmarks/queries/query_17.sql) | 414 B | 146.45 | 145.98 | 146.00 | 145.32 | 146.60 | ok |
| [Q18](benchmarks/queries/query_18.sql) | 264 B | 371.65 | 354.57 | 349.51 | 345.73 | 373.53 | ok |
| [Q19](benchmarks/queries/query_19.sql) | 0 B | 5.11 | 4.40 | 4.39 | 4.35 | 4.45 | ok |
| [Q20](benchmarks/queries/query_20.sql) | 3 B | 253.57 | 252.47 | 252.69 | 251.74 | 252.77 | ok |
| [Q21](benchmarks/queries/query_21.sql) | 162 B | 275.26 | 277.07 | 277.31 | 274.21 | 279.46 | ok |
| [Q22](benchmarks/queries/query_22.sql) | 6.0 KB | 364.65 | 364.26 | 364.71 | 362.10 | 365.51 | ok |
| [Q23](benchmarks/queries/query_23.sql) | 2.6 KB | 247.58 | 240.46 | 240.62 | 239.89 | 240.72 | ok |
| [Q24](benchmarks/queries/query_24.sql) | 672 B | 221.33 | 220.77 | 220.81 | 220.51 | 220.95 | ok |
| [Q25](benchmarks/queries/query_25.sql) | 382 B | 219.30 | 219.65 | 219.69 | 218.74 | 220.49 | ok |
| [Q26](benchmarks/queries/query_26.sql) | 658 B | 221.11 | 220.32 | 220.03 | 219.98 | 221.26 | ok |
| [Q27](benchmarks/queries/query_27.sql) | 26 B | 448.24 | 450.32 | 450.51 | 448.06 | 452.19 | ok |
| [Q28](benchmarks/queries/query_28.sql) | 556 B | 972.11 | 969.67 | 969.54 | 964.82 | 974.79 | ok |
| [Q29](benchmarks/queries/query_29.sql) | 990 B | 436.39 | 427.32 | 427.54 | 426.26 | 427.95 | ok |
| [Q30](benchmarks/queries/query_30.sql) | 244 B | 237.16 | 235.05 | 235.10 | 234.09 | 235.90 | ok |
| [Q31](benchmarks/queries/query_31.sql) | 398 B | 264.83 | 262.59 | 262.43 | 261.71 | 263.79 | ok |
| [Q32](benchmarks/queries/query_32.sql) | 376 B | 1089.10 | 866.39 | 881.20 | 808.51 | 894.64 | ok |
| [Q33](benchmarks/queries/query_33.sql) | 464 B | 354.03 | 349.81 | 349.73 | 349.37 | 350.43 | ok |
| [Q34](benchmarks/queries/query_34.sql) | 484 B | 369.46 | 435.58 | 420.41 | 370.29 | 531.22 | ok |
| [Q35](benchmarks/queries/query_35.sql) | 493 B | 151.95 | 149.11 | 149.57 | 142.89 | 154.43 | ok |
| [Q36](benchmarks/queries/query_36.sql) | 569 B | 331.43 | 345.67 | 339.52 | 333.21 | 370.41 | ok |
| [Q37](benchmarks/queries/query_37.sql) | 550 B | 249.16 | 247.76 | 247.46 | 246.64 | 249.47 | ok |
| [Q38](benchmarks/queries/query_38.sql) | 480 B | 79.44 | 74.20 | 73.93 | 73.32 | 75.60 | ok |
| [Q39](benchmarks/queries/query_39.sql) | 835 B | 400.94 | 541.20 | 479.52 | 391.72 | 814.03 | ok |
| [Q40](benchmarks/queries/query_40.sql) | 356 B | 103.53 | 95.10 | 95.10 | 94.63 | 95.58 | ok |
| [Q41](benchmarks/queries/query_41.sql) | 132 B | 39.71 | 34.90 | 34.88 | 34.50 | 35.36 | ok |
| [Q42](benchmarks/queries/query_42.sql) | 240 B | 68.37 | 69.46 | 69.69 | 68.04 | 70.42 | ok |
<!-- benchmark-table:end -->
