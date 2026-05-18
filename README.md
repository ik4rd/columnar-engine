# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0](benchmarks/queries/query_0.sql) | 995.90 | 3.83 | 3.68 | 3.12 | 4.84 | ok |
| [1](benchmarks/queries/query_1.sql) | 123.04 | 92.08 | 91.48 | 91.07 | 94.31 | ok |
| [2](benchmarks/queries/query_2.sql) | 30.79 | 17.55 | 17.44 | 17.29 | 18.04 | ok |
| [3](benchmarks/queries/query_3.sql) | 29.63 | 12.42 | 12.42 | 12.34 | 12.50 | ok |
| [4](benchmarks/queries/query_4.sql) | 142.05 | 142.70 | 142.35 | 141.79 | 144.31 | ok |
| [5](benchmarks/queries/query_5.sql) | 138.70 | 124.25 | 124.34 | 123.74 | 124.56 | ok |
| [6](benchmarks/queries/query_6.sql) | 826.80 | 811.41 | 811.37 | 810.87 | 812.03 | ok |
| [7](benchmarks/queries/query_7.sql) | 93.22 | 93.04 | 93.03 | 92.84 | 93.25 | ok |
| [8](benchmarks/queries/query_8.sql) | 284.36 | 268.15 | 268.13 | 266.79 | 269.54 | ok |
| [9](benchmarks/queries/query_9.sql) | 283.58 | 283.47 | 283.50 | 282.66 | 284.22 | ok |
| [10](benchmarks/queries/query_10.sql) | 275.81 | 262.56 | 262.72 | 261.78 | 263.03 | ok |
| [11](benchmarks/queries/query_11.sql) | 281.36 | 270.45 | 269.40 | 267.18 | 275.80 | ok |
| [12](benchmarks/queries/query_12.sql) | 239.44 | 240.14 | 239.91 | 238.99 | 241.77 | ok |
| [13](benchmarks/queries/query_13.sql) | 255.19 | 253.47 | 253.59 | 252.84 | 253.86 | ok |
| [14](benchmarks/queries/query_14.sql) | 272.96 | 263.01 | 259.95 | 258.08 | 274.06 | ok |
| [15](benchmarks/queries/query_15.sql) | 185.02 | 187.16 | 185.72 | 184.86 | 192.34 | ok |
| [16](benchmarks/queries/query_16.sql) | 450.93 | 451.46 | 448.04 | 444.79 | 464.96 | ok |
| [17](benchmarks/queries/query_17.sql) | 504.23 | 502.73 | 502.88 | 499.91 | 505.25 | ok |
| [18](benchmarks/queries/query_18.sql) | 1748.85 | 1721.94 | 1714.67 | 1699.42 | 1759.00 | ok |
| [19](benchmarks/queries/query_19.sql) | 102.34 | 103.17 | 102.95 | 102.84 | 103.96 | ok |
| [20](benchmarks/queries/query_20.sql) | 271.74 | 227.54 | 227.42 | 226.80 | 228.52 | ok |
| [21](benchmarks/queries/query_21.sql) | 251.61 | 250.39 | 249.67 | 249.00 | 253.23 | ok |
| [22](benchmarks/queries/query_22.sql) | 394.47 | 345.59 | 345.71 | 343.67 | 347.27 | ok |
| [23](benchmarks/queries/query_23.sql) | 351.28 | 327.59 | 328.06 | 324.82 | 329.40 | ok |
| [24](benchmarks/queries/query_24.sql) | 449.15 | 446.83 | 446.88 | 446.17 | 447.39 | ok |
| [25](benchmarks/queries/query_25.sql) | 362.02 | 361.44 | 361.39 | 358.95 | 364.01 | ok |
| [26](benchmarks/queries/query_26.sql) | 447.66 | 451.28 | 449.92 | 448.57 | 456.72 | ok |
| [27](benchmarks/queries/query_27.sql) | 555.59 | 543.37 | 543.56 | 540.35 | 546.02 | ok |
| [28](benchmarks/queries/query_28.sql) | 972.12 | 959.65 | 959.59 | 957.18 | 962.25 | ok |
| [29](benchmarks/queries/query_29.sql) | 401.21 | 400.70 | 398.26 | 397.31 | 408.97 | ok |
| [30](benchmarks/queries/query_30.sql) | 288.91 | 272.62 | 272.52 | 271.96 | 273.46 | ok |
| [31](benchmarks/queries/query_31.sql) | 294.57 | 294.57 | 294.58 | 292.24 | 296.87 | ok |
| [32](benchmarks/queries/query_32.sql) | 1371.60 | 1260.21 | 1259.24 | 1247.84 | 1274.52 | ok |
| [33](benchmarks/queries/query_33.sql) | 561.48 | 560.08 | 560.28 | 557.23 | 562.54 | ok |
| [34](benchmarks/queries/query_34.sql) | 610.80 | 603.97 | 603.35 | 601.99 | 607.18 | ok |
| [35](benchmarks/queries/query_35.sql) | 822.76 | 829.71 | 828.24 | 825.27 | 837.10 | ok |
| [36](benchmarks/queries/query_36.sql) | 1134.31 | 1115.40 | 1114.47 | 1113.78 | 1118.89 | ok |
| [37](benchmarks/queries/query_37.sql) | 995.52 | 992.70 | 993.52 | 987.64 | 996.11 | ok |
| [38](benchmarks/queries/query_38.sql) | 575.90 | 552.92 | 552.59 | 551.92 | 554.59 | ok |
| [39](benchmarks/queries/query_39.sql) | 2351.58 | 2405.72 | 2416.35 | 2320.84 | 2469.35 | ok |
| [40](benchmarks/queries/query_40.sql) | 849.25 | 818.38 | 814.49 | 744.99 | 899.55 | ok |
| [41](benchmarks/queries/query_41.sql) | 840.14 | 782.90 | 790.08 | 733.59 | 817.85 | ok |
| [42](benchmarks/queries/query_42.sql) | 1193.24 | 1175.66 | 1169.04 | 1164.91 | 1199.65 | ok |
<!-- benchmark-table:end -->
