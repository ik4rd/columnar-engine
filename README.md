# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0](benchmarks/queries/query_0.sql) | 377.49 | 4.36 | 4.11 | 4.07 | 5.18 | ok |
| [1](benchmarks/queries/query_1.sql) | 28.45 | 6.08 | 6.05 | 5.98 | 6.25 | ok |
| [2](benchmarks/queries/query_2.sql) | 42.29 | 18.92 | 18.11 | 17.36 | 22.09 | ok |
| [3](benchmarks/queries/query_3.sql) | 46.46 | 23.45 | 23.68 | 13.67 | 32.78 | ok |
| [4](benchmarks/queries/query_4.sql) | 174.92 | 215.82 | 180.96 | 156.57 | 344.76 | ok |
| [5](benchmarks/queries/query_5.sql) | 246.48 | 158.39 | 157.15 | 137.11 | 182.14 | ok |
| [6](benchmarks/queries/query_6.sql) | 5.25 | 6.53 | 6.52 | 5.57 | 7.51 | ok |
| [7](benchmarks/queries/query_7.sql) | 13.87 | 7.83 | 7.75 | 7.42 | 8.42 | ok |
| [8](benchmarks/queries/query_8.sql) | 283.39 | 267.85 | 251.33 | 246.69 | 322.07 | ok |
| [9](benchmarks/queries/query_9.sql) | 250.66 | 314.76 | 289.02 | 264.74 | 416.26 | ok |
| [10](benchmarks/queries/query_10.sql) | 334.32 | 319.73 | 280.91 | 264.38 | 452.73 | ok |
| [11](benchmarks/queries/query_11.sql) | 285.33 | 265.29 | 265.35 | 264.29 | 266.15 | ok |
| [12](benchmarks/queries/query_12.sql) | 224.19 | 225.06 | 225.27 | 224.30 | 225.40 | ok |
| [13](benchmarks/queries/query_13.sql) | 238.56 | 238.45 | 238.26 | 237.27 | 239.99 | ok |
| [14](benchmarks/queries/query_14.sql) | 245.11 | 226.99 | 227.10 | 226.07 | 227.69 | ok |
| [15](benchmarks/queries/query_15.sql) | 69.62 | 70.58 | 69.73 | 68.79 | 74.05 | ok |
| [16](benchmarks/queries/query_16.sql) | 116.04 | 116.67 | 116.73 | 115.15 | 118.06 | ok |
| [17](benchmarks/queries/query_17.sql) | 136.02 | 135.58 | 135.61 | 135.16 | 135.96 | ok |
| [18](benchmarks/queries/query_18.sql) | 1111.31 | 1055.16 | 1055.21 | 1049.69 | 1060.53 | ok |
| [19](benchmarks/queries/query_19.sql) | 4.46 | 3.76 | 3.77 | 3.70 | 3.80 | ok |
| [20](benchmarks/queries/query_20.sql) | 203.13 | 132.15 | 132.12 | 131.81 | 132.54 | ok |
| [21](benchmarks/queries/query_21.sql) | 259.42 | 260.83 | 260.76 | 260.46 | 261.32 | ok |
| [22](benchmarks/queries/query_22.sql) | 422.93 | 344.72 | 343.99 | 343.45 | 347.46 | ok |
| [23](benchmarks/queries/query_23.sql) | 258.77 | 223.58 | 223.61 | 222.62 | 224.47 | ok |
| [24](benchmarks/queries/query_24.sql) | 267.35 | 267.28 | 267.32 | 266.13 | 268.35 | ok |
| [25](benchmarks/queries/query_25.sql) | 222.02 | 222.04 | 222.01 | 221.66 | 222.46 | ok |
| [26](benchmarks/queries/query_26.sql) | 268.47 | 267.48 | 267.48 | 267.21 | 267.77 | ok |
| [27](benchmarks/queries/query_27.sql) | 422.02 | 390.91 | 390.36 | 387.81 | 395.11 | ok |
| [28](benchmarks/queries/query_28.sql) | 987.58 | 890.83 | 888.82 | 884.50 | 901.15 | ok |
| [29](benchmarks/queries/query_29.sql) | 416.97 | 435.49 | 414.27 | 408.42 | 505.01 | ok |
| [30](benchmarks/queries/query_30.sql) | 315.06 | 264.87 | 262.45 | 235.27 | 299.32 | ok |
| [31](benchmarks/queries/query_31.sql) | 320.88 | 268.65 | 264.73 | 259.50 | 285.65 | ok |
| [32](benchmarks/queries/query_32.sql) | 1303.27 | 792.57 | 709.36 | 693.41 | 1058.17 | ok |
| [33](benchmarks/queries/query_33.sql) | 429.43 | 360.64 | 357.42 | 355.02 | 372.69 | ok |
| [34](benchmarks/queries/query_34.sql) | 385.04 | 376.68 | 376.93 | 374.49 | 378.36 | ok |
| [35](benchmarks/queries/query_35.sql) | 665.87 | 658.88 | 658.77 | 656.44 | 661.54 | ok |
| [36](benchmarks/queries/query_36.sql) | 353.08 | 317.46 | 315.11 | 314.12 | 325.50 | ok |
| [37](benchmarks/queries/query_37.sql) | 261.66 | 237.02 | 237.26 | 235.48 | 238.09 | ok |
| [38](benchmarks/queries/query_38.sql) | 78.89 | 65.31 | 65.19 | 65.13 | 65.71 | ok |
| [39](benchmarks/queries/query_39.sql) | 474.61 | 402.51 | 401.41 | 399.77 | 407.46 | ok |
| [40](benchmarks/queries/query_40.sql) | 105.05 | 89.50 | 89.50 | 89.44 | 89.53 | ok |
| [41](benchmarks/queries/query_41.sql) | 43.27 | 31.24 | 31.22 | 31.07 | 31.44 | ok |
| [42](benchmarks/queries/query_42.sql) | 558.27 | 571.35 | 568.71 | 562.35 | 585.65 | ok |
<!-- benchmark-table:end -->
