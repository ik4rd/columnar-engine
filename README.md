# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0](benchmarks/queries/query_0.sql) | 879.34 | 4.06 | 4.03 | 3.80 | 4.37 | ok |
| [1](benchmarks/queries/query_1.sql) | 16.98 | 6.37 | 6.44 | 5.80 | 6.83 | ok |
| [2](benchmarks/queries/query_2.sql) | 40.29 | 19.01 | 19.12 | 16.42 | 21.36 | ok |
| [3](benchmarks/queries/query_3.sql) | 33.94 | 16.69 | 13.48 | 13.02 | 26.77 | ok |
| [4](benchmarks/queries/query_4.sql) | 166.78 | 144.97 | 141.47 | 140.19 | 156.76 | ok |
| [5](benchmarks/queries/query_5.sql) | 144.89 | 129.80 | 128.72 | 127.22 | 134.55 | ok |
| [6](benchmarks/queries/query_6.sql) | 931.85 | 879.54 | 803.21 | 793.00 | 1118.75 | ok |
| [7](benchmarks/queries/query_7.sql) | 6.25 | 5.49 | 5.47 | 5.41 | 5.59 | ok |
| [8](benchmarks/queries/query_8.sql) | 216.16 | 198.72 | 198.36 | 197.40 | 200.75 | ok |
| [9](benchmarks/queries/query_9.sql) | 217.73 | 291.19 | 271.79 | 215.18 | 406.03 | ok |
| [10](benchmarks/queries/query_10.sql) | 278.75 | 265.55 | 265.39 | 264.42 | 266.99 | ok |
| [11](benchmarks/queries/query_11.sql) | 279.28 | 265.66 | 265.69 | 265.32 | 265.93 | ok |
| [12](benchmarks/queries/query_12.sql) | 231.17 | 226.67 | 226.69 | 226.06 | 227.22 | ok |
| [13](benchmarks/queries/query_13.sql) | 242.03 | 242.24 | 242.26 | 241.15 | 243.29 | ok |
| [14](benchmarks/queries/query_14.sql) | 247.81 | 230.09 | 229.99 | 229.95 | 230.42 | ok |
| [15](benchmarks/queries/query_15.sql) | 81.68 | 82.23 | 81.93 | 81.39 | 83.68 | ok |
| [16](benchmarks/queries/query_16.sql) | 157.77 | 157.88 | 157.67 | 156.91 | 159.26 | ok |
| [17](benchmarks/queries/query_17.sql) | 256.34 | 263.37 | 262.33 | 256.89 | 271.94 | ok |
| [18](benchmarks/queries/query_18.sql) | 1134.83 | 1104.99 | 1101.99 | 1099.17 | 1116.83 | ok |
| [19](benchmarks/queries/query_19.sql) | 6.37 | 6.30 | 6.27 | 6.12 | 6.57 | ok |
| [20](benchmarks/queries/query_20.sql) | 184.22 | 134.62 | 134.35 | 133.26 | 136.52 | ok |
| [21](benchmarks/queries/query_21.sql) | 263.88 | 270.20 | 269.12 | 264.71 | 277.85 | ok |
| [22](benchmarks/queries/query_22.sql) | 455.59 | 362.59 | 361.44 | 354.48 | 373.00 | ok |
| [23](benchmarks/queries/query_23.sql) | 266.16 | 231.38 | 231.43 | 230.95 | 231.72 | ok |
| [24](benchmarks/queries/query_24.sql) | 269.60 | 269.43 | 269.41 | 269.09 | 269.80 | ok |
| [25](benchmarks/queries/query_25.sql) | 222.75 | 223.28 | 222.72 | 221.34 | 226.33 | ok |
| [26](benchmarks/queries/query_26.sql) | 275.85 | 299.21 | 278.51 | 268.94 | 370.87 | ok |
| [27](benchmarks/queries/query_27.sql) | 480.20 | 428.84 | 422.07 | 418.66 | 452.56 | ok |
| [28](benchmarks/queries/query_28.sql) | 1014.03 | 986.19 | 967.72 | 939.46 | 1069.87 | ok |
| [29](benchmarks/queries/query_29.sql) | 385.86 | 387.94 | 387.48 | 385.77 | 391.04 | ok |
| [30](benchmarks/queries/query_30.sql) | 261.99 | 233.74 | 233.39 | 232.63 | 235.56 | ok |
| [31](benchmarks/queries/query_31.sql) | 270.36 | 259.06 | 258.89 | 256.51 | 261.96 | ok |
| [32](benchmarks/queries/query_32.sql) | 797.67 | 766.59 | 765.74 | 761.95 | 772.95 | ok |
| [33](benchmarks/queries/query_33.sql) | 392.45 | 386.23 | 385.99 | 385.40 | 387.54 | ok |
| [34](benchmarks/queries/query_34.sql) | 417.36 | 418.05 | 417.09 | 415.47 | 422.57 | ok |
| [35](benchmarks/queries/query_35.sql) | 713.31 | 711.97 | 712.85 | 708.75 | 713.42 | ok |
| [36](benchmarks/queries/query_36.sql) | 413.04 | 380.49 | 380.77 | 378.52 | 381.89 | ok |
| [37](benchmarks/queries/query_37.sql) | 355.23 | 326.47 | 326.38 | 325.73 | 327.41 | ok |
| [38](benchmarks/queries/query_38.sql) | 135.30 | 119.23 | 119.34 | 118.83 | 119.41 | ok |
| [39](benchmarks/queries/query_39.sql) | 589.40 | 541.19 | 543.41 | 532.23 | 545.69 | ok |
| [40](benchmarks/queries/query_40.sql) | 149.01 | 123.71 | 123.69 | 123.42 | 124.03 | ok |
| [41](benchmarks/queries/query_41.sql) | 58.34 | 43.26 | 43.21 | 42.85 | 43.77 | ok |
| [42](benchmarks/queries/query_42.sql) | 571.88 | 570.13 | 569.36 | 567.66 | 574.16 | ok |
<!-- benchmark-table:end -->
