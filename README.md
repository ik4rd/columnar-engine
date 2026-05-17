# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0](benchmarks/queries/query_0.sql) | 55.59 | 16.33 | 16.79 | 13.30 | 18.44 | ok |
| [1](benchmarks/queries/query_1.sql) | 48.77 | 47.12 | 47.61 | 40.33 | 52.91 | ok |
| [2](benchmarks/queries/query_2.sql) | 84.36 | 111.30 | 83.91 | 69.00 | 208.37 | ok |
| [3](benchmarks/queries/query_3.sql) | 101.25 | 77.98 | 69.68 | 59.56 | 112.99 | ok |
| [4](benchmarks/queries/query_4.sql) | 66.55 | 58.00 | 53.02 | 49.21 | 76.76 | ok |
| [5](benchmarks/queries/query_5.sql) | 62.91 | 50.39 | 49.26 | 48.82 | 54.22 | ok |
| [6](benchmarks/queries/query_6.sql) | 1194.71 | 1014.77 | 923.37 | 697.76 | 1514.56 | ok |
| [7](benchmarks/queries/query_7.sql) | 30.13 | 30.41 | 30.41 | 30.06 | 30.75 | ok |
| [8](benchmarks/queries/query_8.sql) | 141.16 | 263.37 | 256.92 | 183.97 | 355.67 | ok |
| [9](benchmarks/queries/query_9.sql) | 184.06 | 173.96 | 172.43 | 168.05 | 182.94 | ok |
| [10](benchmarks/queries/query_10.sql) | 49.58 | 44.81 | 44.16 | 40.82 | 50.11 | ok |
| [11](benchmarks/queries/query_11.sql) | 52.93 | 39.66 | 39.58 | 39.36 | 40.13 | ok |
| [12](benchmarks/queries/query_12.sql) | 69.12 | 65.21 | 65.11 | 64.91 | 65.73 | ok |
| [13](benchmarks/queries/query_13.sql) | 76.07 | 74.94 | 74.85 | 74.42 | 75.62 | ok |
| [14](benchmarks/queries/query_14.sql) | 82.87 | 72.19 | 71.66 | 70.94 | 74.48 | ok |
| [15](benchmarks/queries/query_15.sql) | 104.55 | 107.21 | 105.36 | 101.76 | 116.37 | ok |
| [16](benchmarks/queries/query_16.sql) | 200.52 | 207.87 | 205.56 | 203.79 | 216.57 | ok |
| [17](benchmarks/queries/query_17.sql) | 191.47 | 204.08 | 191.23 | 189.18 | 244.68 | ok |
| [18](benchmarks/queries/query_18.sql) | — | — | — | — | — | failed: executor: expected ')' after aggregate arguments |
| [19](benchmarks/queries/query_19.sql) | — | — | — | — | — | failed: executor: plain column SELECT items require GRO… |
| [20](benchmarks/queries/query_20.sql) | — | — | — | — | — | failed: executor: expected comparison operator in WHERE |
| [21](benchmarks/queries/query_21.sql) | — | — | — | — | — | failed: executor: expected comparison operator in WHERE |
| [22](benchmarks/queries/query_22.sql) | — | — | — | — | — | failed: executor: expected comparison operator in WHERE |
| [23](benchmarks/queries/query_23.sql) | — | — | — | — | — | failed: executor: expected '(' after aggregate function |
| [24](benchmarks/queries/query_24.sql) | — | — | — | — | — | failed: executor: plain column SELECT items require GRO… |
| [25](benchmarks/queries/query_25.sql) | — | — | — | — | — | failed: executor: plain column SELECT items require GRO… |
| [26](benchmarks/queries/query_26.sql) | — | — | — | — | — | failed: executor: plain column SELECT items require GRO… |
| [27](benchmarks/queries/query_27.sql) | — | — | — | — | — | failed: executor: expected ')' after aggregate arguments |
| [28](benchmarks/queries/query_28.sql) | — | — | — | — | — | failed: executor: expected ')' after aggregate arguments |
| [29](benchmarks/queries/query_29.sql) | — | — | — | — | — | failed: executor: expected ')' after aggregate arguments |
| [30](benchmarks/queries/query_30.sql) | 105.20 | 81.72 | 81.93 | 80.76 | 82.26 | ok |
| [31](benchmarks/queries/query_31.sql) | 123.33 | 120.00 | 120.22 | 117.07 | 122.50 | ok |
| [32](benchmarks/queries/query_32.sql) | 1191.34 | 1133.03 | 1138.44 | 1059.16 | 1196.10 | ok |
| [33](benchmarks/queries/query_33.sql) | 453.81 | 411.95 | 409.21 | 404.40 | 424.97 | ok |
| [34](benchmarks/queries/query_34.sql) | — | — | — | — | — | failed: executor: expected aggregate function |
| [35](benchmarks/queries/query_35.sql) | — | — | — | — | — | failed: executor: expected FROM |
| [36](benchmarks/queries/query_36.sql) | — | — | — | — | — | failed: executor: unexpected trailing tokens |
| [37](benchmarks/queries/query_37.sql) | — | — | — | — | — | failed: executor: unexpected trailing tokens |
| [38](benchmarks/queries/query_38.sql) | — | — | — | — | — | failed: executor: unexpected trailing tokens |
| [39](benchmarks/queries/query_39.sql) | — | — | — | — | — | failed: executor: expected FROM |
| [40](benchmarks/queries/query_40.sql) | — | — | — | — | — | failed: executor: unexpected trailing tokens |
| [41](benchmarks/queries/query_41.sql) | — | — | — | — | — | failed: executor: unexpected trailing tokens |
| [42](benchmarks/queries/query_42.sql) | — | — | — | — | — | failed: executor: expected column name in aggregate |
<!-- benchmark-table:end -->
