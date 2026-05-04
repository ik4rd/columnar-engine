# columnar-engine

Колононый движок с нуля (in progress)

<!-- benchmark-table:start -->
## Dashboard


| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [query_0.sql](benchmarks/queries/query_0.sql) | 28.86 | 12.14 | 11.25 | 9.60 | 16.44 | ok |
| [query_1.sql](benchmarks/queries/query_1.sql) | 51.92 | 30.69 | 30.18 | 29.77 | 32.64 | ok |
| [query_2.sql](benchmarks/queries/query_2.sql) | 72.78 | 58.04 | 58.82 | 54.30 | 60.23 | ok |
| [query_3.sql](benchmarks/queries/query_3.sql) | 74.22 | 77.95 | 67.37 | 52.85 | 124.19 | ok |
| [query_4.sql](benchmarks/queries/query_4.sql) | 68.50 | 52.63 | 51.18 | 48.12 | 60.06 | ok |
| [query_5.sql](benchmarks/queries/query_5.sql) | 60.41 | 44.76 | 44.68 | 44.65 | 45.02 | ok |
| [query_6.sql](benchmarks/queries/query_6.sql) | 693.40 | 675.22 | 665.84 | 663.16 | 706.04 | ok |
| [query_7.sql](benchmarks/queries/query_7.sql) | 31.60 | 29.68 | 29.62 | 28.63 | 30.84 | ok |
| [query_8.sql](benchmarks/queries/query_8.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_9.sql](benchmarks/queries/query_9.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_10.sql](benchmarks/queries/query_10.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_11.sql](benchmarks/queries/query_11.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_12.sql](benchmarks/queries/query_12.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_13.sql](benchmarks/queries/query_13.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_14.sql](benchmarks/queries/query_14.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_15.sql](benchmarks/queries/query_15.sql) | — | — | — | — | — | failed: query_parser: unexpected trailing tokens |
| [query_16.sql](benchmarks/queries/query_16.sql) | — | — | — | — | — | failed: query_parser: unexpected trailing tokens |
| [query_17.sql](benchmarks/queries/query_17.sql) | — | — | — | — | — | failed: query_parser: unexpected trailing tokens |
| [query_18.sql](benchmarks/queries/query_18.sql) | — | — | — | — | — | failed: executor: aggregate 'extract' is not registered |
| [query_19.sql](benchmarks/queries/query_19.sql) | — | — | — | — | — | failed: query_planner: plain column SELECT items requir… |
| [query_20.sql](benchmarks/queries/query_20.sql) | — | — | — | — | — | failed: query_parser: expected comparison operator in W… |
| [query_21.sql](benchmarks/queries/query_21.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_22.sql](benchmarks/queries/query_22.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_23.sql](benchmarks/queries/query_23.sql) | — | — | — | — | — | failed: executor: aggregate '*' is not registered |
| [query_24.sql](benchmarks/queries/query_24.sql) | — | — | — | — | — | failed: query_parser: unexpected trailing tokens |
| [query_25.sql](benchmarks/queries/query_25.sql) | — | — | — | — | — | failed: query_parser: unexpected trailing tokens |
| [query_26.sql](benchmarks/queries/query_26.sql) | — | — | — | — | — | failed: query_parser: unexpected trailing tokens |
| [query_27.sql](benchmarks/queries/query_27.sql) | — | — | — | — | — | failed: query_parser: expected ')' after aggregate argu… |
| [query_28.sql](benchmarks/queries/query_28.sql) | — | — | — | — | — | failed: executor: aggregate 'REGEXP_REPLACE' is not reg… |
| [query_29.sql](benchmarks/queries/query_29.sql) | — | — | — | — | — | failed: query_parser: expected ')' after aggregate argu… |
| [query_30.sql](benchmarks/queries/query_30.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_31.sql](benchmarks/queries/query_31.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_32.sql](benchmarks/queries/query_32.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_33.sql](benchmarks/queries/query_33.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_34.sql](benchmarks/queries/query_34.sql) | — | — | — | — | — | failed: query_parser: expected aggregate function |
| [query_35.sql](benchmarks/queries/query_35.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_36.sql](benchmarks/queries/query_36.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_37.sql](benchmarks/queries/query_37.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_38.sql](benchmarks/queries/query_38.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_39.sql](benchmarks/queries/query_39.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_40.sql](benchmarks/queries/query_40.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_41.sql](benchmarks/queries/query_41.sql) | — | — | — | — | — | failed: query_parser: expected FROM |
| [query_42.sql](benchmarks/queries/query_42.sql) | — | — | — | — | — | failed: executor: aggregate 'DATE_TRUNC' is not registe… |
<!-- benchmark-table:end -->
