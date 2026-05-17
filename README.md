# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0](benchmarks/queries/query_0.sql) | 2074.67 | 811.17 | 810.14 | 799.19 | 825.20 | ok |
| [1](benchmarks/queries/query_1.sql) | 3960.96 | 4117.56 | 4068.44 | 4058.21 | 4275.13 | ok |
| [2](benchmarks/queries/query_2.sql) | 5774.39 | 5732.02 | 5720.86 | 5714.63 | 5771.71 | ok |
| [3](benchmarks/queries/query_3.sql) | 1351.80 | 1347.59 | 1347.09 | 1342.99 | 1353.20 | ok |
| [4](benchmarks/queries/query_4.sql) | 1354.22 | 1348.45 | 1348.73 | 1346.07 | 1350.26 | ok |
| [5](benchmarks/queries/query_5.sql) | 3966.57 | 3994.29 | 3988.21 | 3981.10 | 4019.63 | ok |
| [6](benchmarks/queries/query_6.sql) | 2304.40 | 2301.01 | 2300.10 | 2294.89 | 2308.95 | ok |
| [7](benchmarks/queries/query_7.sql) | 4047.89 | 4069.98 | 4059.09 | 4048.81 | 4112.94 | ok |
| [8](benchmarks/queries/query_8.sql) | 1989.25 | 1989.12 | 1989.14 | 1984.81 | 1993.39 | ok |
| [9](benchmarks/queries/query_9.sql) | 6977.09 | 7020.60 | 6992.05 | 6977.03 | 7121.29 | ok |
| [10](benchmarks/queries/query_10.sql) | 4172.85 | 4174.64 | 4173.62 | 4166.41 | 4184.90 | ok |
| [11](benchmarks/queries/query_11.sql) | 4219.03 | 4263.52 | 4268.62 | 4225.07 | 4291.76 | ok |
| [12](benchmarks/queries/query_12.sql) | 4593.44 | 4594.19 | 4594.18 | 4589.27 | 4599.11 | ok |
| [13](benchmarks/queries/query_13.sql) | 4653.27 | 4658.07 | 4645.18 | 4643.49 | 4698.43 | ok |
| [14](benchmarks/queries/query_14.sql) | 4853.63 | 4870.43 | 4844.51 | 4837.47 | 4955.24 | ok |
| [15](benchmarks/queries/query_15.sql) | 1448.92 | 1459.48 | 1458.77 | 1450.48 | 1469.91 | ok |
| [16](benchmarks/queries/query_16.sql) | 4734.09 | 4760.53 | 4743.25 | 4740.66 | 4814.96 | ok |
| [17](benchmarks/queries/query_17.sql) | 4871.81 | 4806.35 | 4806.42 | 4801.99 | 4810.56 | ok |
| [18](benchmarks/queries/query_18.sql) | 6183.75 | 6178.21 | 6178.31 | 6168.44 | 6187.76 | ok |
| [19](benchmarks/queries/query_19.sql) | — | — | — | — | — | failed: io: schema has no columns |
| [20](benchmarks/queries/query_20.sql) | 6143.00 | 6131.75 | 6129.31 | 6120.90 | 6147.46 | ok |
| [21](benchmarks/queries/query_21.sql) | 6157.59 | 6228.70 | 6203.56 | 6163.38 | 6344.31 | ok |
| [22](benchmarks/queries/query_22.sql) | 7971.06 | 7967.92 | 7954.93 | 7926.64 | 8035.18 | ok |
| [23](benchmarks/queries/query_23.sql) | 6202.16 | 6159.66 | 6155.30 | 6145.65 | 6182.40 | ok |
| [24](benchmarks/queries/query_24.sql) | 5132.71 | 4929.16 | 4930.03 | 4913.20 | 4943.36 | ok |
| [25](benchmarks/queries/query_25.sql) | 4958.52 | 4984.53 | 4977.69 | 4932.86 | 5049.88 | ok |
| [26](benchmarks/queries/query_26.sql) | 5041.04 | 4995.97 | 4996.44 | 4990.23 | 5000.76 | ok |
| [27](benchmarks/queries/query_27.sql) | 7382.97 | 7372.01 | 7368.33 | 7353.54 | 7397.84 | ok |
| [28](benchmarks/queries/query_28.sql) | 14726.43 | 14708.99 | 14705.53 | 14678.31 | 14746.58 | ok |
| [29](benchmarks/queries/query_29.sql) | 168947.94 | 169196.08 | 168910.28 | 168813.05 | 170150.73 | ok |
| [30](benchmarks/queries/query_30.sql) | 4954.02 | 4837.42 | 4837.53 | 4832.97 | 4841.63 | ok |
| [31](benchmarks/queries/query_31.sql) | 4661.18 | 4644.13 | 4643.28 | 4641.72 | 4648.24 | ok |
| [32](benchmarks/queries/query_32.sql) | 5530.30 | 5421.30 | 5421.54 | 5385.94 | 5456.20 | ok |
| [33](benchmarks/queries/query_33.sql) | 1838.69 | 1838.33 | 1832.66 | 1831.11 | 1856.87 | ok |
| [34](benchmarks/queries/query_34.sql) | 1917.73 | 1875.79 | 1874.57 | 1871.86 | 1882.16 | ok |
| [35](benchmarks/queries/query_35.sql) | 3202.07 | 3204.65 | 3203.29 | 3201.79 | 3210.23 | ok |
| [36](benchmarks/queries/query_36.sql) | 7140.35 | 7126.02 | 7127.07 | 7115.60 | 7134.34 | ok |
| [37](benchmarks/queries/query_37.sql) | 6604.60 | 6586.07 | 6584.74 | 6575.14 | 6599.65 | ok |
| [38](benchmarks/queries/query_38.sql) | 4084.18 | 4079.88 | 4080.28 | 4077.60 | 4081.37 | ok |
| [39](benchmarks/queries/query_39.sql) | 12156.94 | 12174.77 | 12184.77 | 12099.58 | 12229.96 | ok |
| [40](benchmarks/queries/query_40.sql) | 7019.52 | 6999.76 | 6997.49 | 6984.79 | 7019.29 | ok |
| [41](benchmarks/queries/query_41.sql) | 7930.25 | 7918.42 | 7917.43 | 7910.02 | 7928.78 | ok |
| [42](benchmarks/queries/query_42.sql) | 7037.57 | 7038.40 | 7028.24 | 7013.77 | 7083.36 | ok |
<!-- benchmark-table:end -->
