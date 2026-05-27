# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0](benchmarks/queries/query_0.sql) | 344.82 | 3.58 | 3.28 | 3.12 | 4.65 | ok |
| [1](benchmarks/queries/query_1.sql) | 15.22 | 5.36 | 5.32 | 5.25 | 5.57 | ok |
| [2](benchmarks/queries/query_2.sql) | 27.79 | 16.53 | 16.14 | 16.06 | 17.75 | ok |
| [3](benchmarks/queries/query_3.sql) | 32.28 | 13.58 | 13.58 | 13.28 | 13.88 | ok |
| [4](benchmarks/queries/query_4.sql) | 151.52 | 160.60 | 160.05 | 141.63 | 180.66 | ok |
| [5](benchmarks/queries/query_5.sql) | 140.53 | 127.81 | 127.23 | 126.06 | 130.71 | ok |
| [6](benchmarks/queries/query_6.sql) | 29.23 | 15.53 | 15.49 | 15.35 | 15.79 | ok |
| [7](benchmarks/queries/query_7.sql) | 5.81 | 5.48 | 5.47 | 5.41 | 5.55 | ok |
| [8](benchmarks/queries/query_8.sql) | 216.79 | 196.14 | 195.96 | 195.86 | 196.77 | ok |
| [9](benchmarks/queries/query_9.sql) | 211.65 | 212.82 | 212.72 | 211.95 | 213.88 | ok |
| [10](benchmarks/queries/query_10.sql) | 280.35 | 261.26 | 261.18 | 260.89 | 261.80 | ok |
| [11](benchmarks/queries/query_11.sql) | 277.78 | 268.03 | 268.03 | 263.85 | 272.23 | ok |
| [12](benchmarks/queries/query_12.sql) | 240.00 | 232.74 | 234.75 | 225.59 | 235.87 | ok |
| [13](benchmarks/queries/query_13.sql) | 239.58 | 245.62 | 244.66 | 239.40 | 253.78 | ok |
| [14](benchmarks/queries/query_14.sql) | 251.66 | 233.72 | 233.86 | 228.91 | 238.23 | ok |
| [15](benchmarks/queries/query_15.sql) | 81.63 | 83.01 | 81.83 | 81.29 | 87.08 | ok |
| [16](benchmarks/queries/query_16.sql) | 152.52 | 153.13 | 153.09 | 151.13 | 155.19 | ok |
| [17](benchmarks/queries/query_17.sql) | 267.83 | 262.55 | 262.71 | 259.32 | 265.45 | ok |
| [18](benchmarks/queries/query_18.sql) | 1131.35 | 1089.81 | 1081.10 | 1076.09 | 1120.95 | ok |
| [19](benchmarks/queries/query_19.sql) | 6.54 | 6.08 | 6.09 | 6.01 | 6.13 | ok |
| [20](benchmarks/queries/query_20.sql) | 174.17 | 131.71 | 131.80 | 131.29 | 131.93 | ok |
| [21](benchmarks/queries/query_21.sql) | 260.50 | 261.22 | 261.27 | 260.70 | 261.65 | ok |
| [22](benchmarks/queries/query_22.sql) | 436.67 | 349.52 | 349.34 | 348.74 | 350.65 | ok |
| [23](benchmarks/queries/query_23.sql) | 255.62 | 238.82 | 231.81 | 231.16 | 260.50 | ok |
| [24](benchmarks/queries/query_24.sql) | 263.80 | 264.23 | 264.22 | 264.09 | 264.37 | ok |
| [25](benchmarks/queries/query_25.sql) | 218.05 | 218.55 | 218.51 | 217.92 | 219.24 | ok |
| [26](benchmarks/queries/query_26.sql) | 264.13 | 264.57 | 264.09 | 262.94 | 267.18 | ok |
| [27](benchmarks/queries/query_27.sql) | 418.65 | 410.47 | 410.24 | 404.88 | 416.52 | ok |
| [28](benchmarks/queries/query_28.sql) | 971.41 | 945.91 | 935.26 | 919.45 | 993.66 | ok |
| [29](benchmarks/queries/query_29.sql) | 382.91 | 382.63 | 382.34 | 381.42 | 384.41 | ok |
| [30](benchmarks/queries/query_30.sql) | 244.12 | 229.83 | 229.60 | 229.24 | 230.87 | ok |
| [31](benchmarks/queries/query_31.sql) | 256.75 | 260.12 | 260.22 | 257.54 | 262.48 | ok |
| [32](benchmarks/queries/query_32.sql) | 1159.79 | 1091.79 | 975.76 | 759.87 | 1655.75 | ok |
| [33](benchmarks/queries/query_33.sql) | 388.19 | 395.00 | 393.90 | 382.80 | 409.40 | ok |
| [34](benchmarks/queries/query_34.sql) | 452.07 | 433.22 | 434.69 | 418.61 | 444.88 | ok |
| [35](benchmarks/queries/query_35.sql) | 721.51 | 735.96 | 738.79 | 716.18 | 750.08 | ok |
| [36](benchmarks/queries/query_36.sql) | 403.64 | 381.15 | 380.76 | 377.53 | 385.53 | ok |
| [37](benchmarks/queries/query_37.sql) | 345.92 | 333.38 | 332.43 | 327.14 | 341.50 | ok |
| [38](benchmarks/queries/query_38.sql) | 138.64 | 122.02 | 122.05 | 120.35 | 123.64 | ok |
| [39](benchmarks/queries/query_39.sql) | 581.40 | 569.20 | 573.40 | 542.29 | 587.73 | ok |
| [40](benchmarks/queries/query_40.sql) | 155.10 | 133.07 | 133.77 | 126.67 | 138.07 | ok |
| [41](benchmarks/queries/query_41.sql) | 65.11 | 47.84 | 46.98 | 44.92 | 52.48 | ok |
| [42](benchmarks/queries/query_42.sql) | 738.49 | 587.37 | 583.54 | 577.32 | 605.10 | ok |
<!-- benchmark-table:end -->
