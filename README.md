# columnar-engine

![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)
![CMake](https://img.shields.io/badge/CMake-%E2%89%A53.25-064F8C.svg)
![ClickBench](https://img.shields.io/badge/ClickBench-43%2F43%20ok-success.svg)
![Compression](https://img.shields.io/badge/compression-LZ4-orange.svg)

**Аналитический колоночный движок, написанный с нуля на C++23.**

Собственный колоночный формат хранения с LZ4-сжатием, конвертер CSV ⇄ columnar, SQL-парсер и векторизованный исполнитель
запросов.

Производительность проверяется на наборе запросов [ClickBench](https://github.com/ClickHouse/ClickBench) (
датасет `hits`).

---

## Архитектура

| Модуль        | Библиотека                                    | Назначение                                                    |
|---------------|-----------------------------------------------|---------------------------------------------------------------|
| `model/`      | `columnar_engine_core`                        | Колонки, батчи, схема, метаданные                             |
| `io/`         | `columnar_engine_core` / `_columnar` / `_csv` | Файловый I/O, компрессия, чтение и запись columnar/CSV батчей |
| `convert/`    | `columnar_engine_convert`                     | Конвертация CSV ⇄ columnar                                    |
| `sql_parser/` | `columnar_engine_executor`                    | Токенизатор SQL                                               |
| `executor/`   | `columnar_engine_executor`                    | Парсер запросов, планировщик, операторы, агрегаты             |
| `common/`     | `columnar_engine_core`                        | Ошибки, парсинг, int128, string arena, утилиты                |
| `app/`        | `columnar_engine` (исполняемый файл)          | CLI                                                           |

## Сборка

### Зависимости

- компилятор с поддержкой C++23 (Clang / GCC)
- CMake ≥ 3.25
- liblz4 (`brew install lz4` / `apt install liblz4-dev`)
- GoogleTest — только для тестов (без него тесты отключаются автоматически)
- `argparse` скачивается через `FetchContent` сам

### Быстрый старт

```bash
./script/build.sh
```

## Использование

**1. Вывести схему из CSV:**

```bash
build/src/columnar_engine infer-schema \
  --input data/hits.csv \
  --output data/schema.csv
```

**2. Сконвертировать CSV в колоночный формат:**

```bash
build/src/columnar_engine convert \
  --schema data/schema.csv \
  --input data/hits.csv \
  --output data/hits.columnar \
  --row-group-size 16384 \
  --compression lz4
```

**3. Выполнить SQL-запрос:**

```bash
build/src/columnar_engine run-query \
  --input data/hits.columnar \
  --table-name hits \
  --query "SELECT COUNT(*) FROM hits" \
  --output result.csv
```

Вместо `--query` можно передать файл.

**4. Обратная конвертация в CSV:**

```bash
build/src/columnar_engine roundtrip \
  --input data/hits.columnar \
  --schema-output out/schema.csv \
  --csv-output out/hits.csv
```

## Тестирование

| Тест                         | Что проверяет                                               |
|------------------------------|-------------------------------------------------------------|
| `test_columns`               | Колонки всех типов                                          |
| `test_batch`                 | Батчи и работа с ними                                       |
| `test_fileio`, `test_stream` | Файловый и потоковый I/O                                    |
| `test_columnar`              | Колоночный формат: запись/чтение, row group'ы, компрессия   |
| `test_csv`                   | CSV-парсер и writer, вывод схемы                            |
| `test_tokenizer`             | SQL-токенизатор                                             |
| `test_executor`              | Парсер запросов, планировщик, операторы, end-to-end запросы |

Запуск:

```bash
cmake -S . -B build -DENABLE_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

### Санитайзеры

Отдельный таргет собирает проект в `build-sanitizers/` с ASan + UBSan и прогоняет все тесты:

```bash
cmake --build build --target test_sanitizers
```

## Бенчмарки

Бенчмарки гоняют запросы ClickBench (`benchmarks/queries/query_*.sql`) на сэмпле датасета `hits`:

```bash
build/benchmarks/benchmark_csv_to_columnar

build/benchmarks/benchmark_queries --input benchmarks/hits_sample.columnar
```

Дашборд ниже генерируется автоматически:

```bash
python3 scripts/dashboard.py
```

---

## Среда выполнения benchmark'ов

- Устройство: MacBook Pro (`MacBookPro17,1`)
- Процессор: Apple M1
- Ядра CPU: 8 (`4` performance + `4` efficiency)
- Оперативная память: 8 GB
- Архитектура: `arm64`
- ОС: macOS `26.3` (`25D5101c`)

<!-- benchmark-table:start -->

## Benchmark Dashboard

`generated 2026-06-04 16:20:50` · `queries 43/43` · `warm runs 4` · `cache drop per-query` · `query mode hardcoded` · [
`csv`](benchmarks/results/readme_benchmarks.csv)

<table>
<tr>
<td valign="top" width="50%">
<h3>Summary</h3>
<table>
<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>
<tbody>
<tr><td>queries ok</td><td align="right">43 / 43</td></tr>
<tr><td>queries failed</td><td align="right">0</td></tr>
<tr><td>median warm time</td><td align="right">105.00 ms</td></tr>
<tr><td>average warm time</td><td align="right">147.59 ms</td></tr>
<tr><td>p95 warm time</td><td align="right">389.85 ms</td></tr>
<tr><td>cold/warm delta</td><td align="right">20.3%</td></tr>
<tr><td>total output size</td><td align="right">0 B</td></tr>
<tr><td>max output size</td><td align="right">0 B</td></tr>
<tr><td>fastest query</td><td align="right">Q00 · &lt;1 ms</td></tr>
<tr><td>slowest query</td><td align="right">Q28 · 873.00 ms</td></tr>
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
<tr><td>csv -&gt; columnar</td><td align="right">5.87s</td></tr>
<tr><td>columnar -&gt; csv</td><td align="right">9.11s</td></tr>
<tr><td>convert throughput</td><td align="right">136.6 MB/s</td></tr>
<tr><td>roundtrip throughput</td><td align="right">13.1 MB/s</td></tr>
</tbody>
</table>
</td>
</tr>
</table>

### Heatmap

`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` <1 ms

|          slot 1 |          slot 2 |          slot 3 |          slot 4 |          slot 5 |          slot 6 |
|----------------:|----------------:|----------------:|----------------:|----------------:|----------------:|
|      Q00 ⬜ `<1` |   Q01 🟩 `4.00` |   Q02 🟩 `6.50` |   Q03 🟩 `6.00` |  Q04 🟩 `35.50` |  Q05 🟩 `25.00` |
|      Q06 ⬜ `<1` |   Q07 🟩 `7.00` | Q08 🟥 `170.00` | Q09 🟥 `187.50` | Q10 🟥 `258.00` | Q11 🟥 `257.00` |
| Q12 🟥 `210.00` | Q13 🟥 `243.00` | Q14 🟥 `211.50` |  Q15 🟩 `54.00` |  Q16 🟩 `84.00` | Q17 🟥 `123.00` |
| Q18 🟥 `346.00` |      Q19 ⬜ `<1` |  Q20 🟦 `98.50` | Q21 🟦 `111.50` | Q22 🟥 `178.00` | Q23 🟥 `156.00` |
|  Q24 🟩 `16.00` |  Q25 🟩 `12.00` |  Q26 🟩 `16.00` | Q27 🟥 `389.50` | Q28 🟥 `873.00` |   Q29 🟩 `4.00` |
|  Q30 🟩 `24.00` |  Q31 🟩 `36.00` | Q32 🟥 `396.50` | Q33 🟥 `195.50` | Q34 🟥 `189.00` |  Q35 🟩 `19.50` |
| Q36 🟥 `272.50` | Q37 🟥 `197.50` |  Q38 🟩 `46.00` | Q39 🟥 `309.00` |  Q40 🟩 `38.50` |  Q41 🟩 `27.00` |
|  Q42 🟩 `70.00` |                 |                 |                 |                 |                 |

### Query Table

| Query                                  | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |
|----------------------------------------|-----------:|--------------:|-------------:|----------------:|-------------:|-------------:|--------|
| [Q00](benchmarks/queries/query_0.sql)  |        0 B |            <1 |           <1 |              <1 |           <1 |           <1 | ok     |
| [Q01](benchmarks/queries/query_1.sql)  |        0 B |         17.00 |         4.00 |            4.00 |         4.00 |         4.00 | ok     |
| [Q02](benchmarks/queries/query_2.sql)  |        0 B |         28.00 |         6.50 |            6.50 |         6.00 |         7.00 | ok     |
| [Q03](benchmarks/queries/query_3.sql)  |        0 B |         21.00 |         6.00 |            6.00 |         6.00 |         6.00 | ok     |
| [Q04](benchmarks/queries/query_4.sql)  |        0 B |         50.00 |        35.50 |           35.50 |        35.00 |        36.00 | ok     |
| [Q05](benchmarks/queries/query_5.sql)  |        0 B |         46.00 |        25.25 |           25.00 |        25.00 |        26.00 | ok     |
| [Q06](benchmarks/queries/query_6.sql)  |        0 B |            <1 |           <1 |              <1 |           <1 |           <1 | ok     |
| [Q07](benchmarks/queries/query_7.sql)  |        0 B |         17.00 |         7.00 |            7.00 |         7.00 |         7.00 | ok     |
| [Q08](benchmarks/queries/query_8.sql)  |        0 B |        190.00 |       170.25 |          170.00 |       169.00 |       172.00 | ok     |
| [Q09](benchmarks/queries/query_9.sql)  |        0 B |        231.00 |       188.25 |          187.50 |       186.00 |       192.00 | ok     |
| [Q10](benchmarks/queries/query_10.sql) |        0 B |        296.00 |       258.00 |          258.00 |       256.00 |       260.00 | ok     |
| [Q11](benchmarks/queries/query_11.sql) |        0 B |        293.00 |       257.50 |          257.00 |       257.00 |       259.00 | ok     |
| [Q12](benchmarks/queries/query_12.sql) |        0 B |        236.00 |       209.50 |          210.00 |       206.00 |       212.00 | ok     |
| [Q13](benchmarks/queries/query_13.sql) |        0 B |        271.00 |       248.75 |          243.00 |       239.00 |       270.00 | ok     |
| [Q14](benchmarks/queries/query_14.sql) |        0 B |        251.00 |       211.25 |          211.50 |       209.00 |       213.00 | ok     |
| [Q15](benchmarks/queries/query_15.sql) |        0 B |         70.00 |        54.25 |           54.00 |        52.00 |        57.00 | ok     |
| [Q16](benchmarks/queries/query_16.sql) |        0 B |        120.00 |        84.75 |           84.00 |        83.00 |        88.00 | ok     |
| [Q17](benchmarks/queries/query_17.sql) |        0 B |        166.00 |       123.00 |          123.00 |       122.00 |       124.00 | ok     |
| [Q18](benchmarks/queries/query_18.sql) |        0 B |        398.00 |       347.50 |          346.00 |       334.00 |       364.00 | ok     |
| [Q19](benchmarks/queries/query_19.sql) |        0 B |          4.00 |           <1 |              <1 |           <1 |           <1 | ok     |
| [Q20](benchmarks/queries/query_20.sql) |        0 B |        157.00 |        98.50 |           98.50 |        98.00 |        99.00 | ok     |
| [Q21](benchmarks/queries/query_21.sql) |        0 B |        181.00 |       111.25 |          111.50 |       109.00 |       113.00 | ok     |
| [Q22](benchmarks/queries/query_22.sql) |        0 B |        273.00 |       178.50 |          178.00 |       176.00 |       182.00 | ok     |
| [Q23](benchmarks/queries/query_23.sql) |        0 B |        284.00 |       155.75 |          156.00 |       155.00 |       156.00 | ok     |
| [Q24](benchmarks/queries/query_24.sql) |        0 B |         64.00 |        16.00 |           16.00 |        16.00 |        16.00 | ok     |
| [Q25](benchmarks/queries/query_25.sql) |        0 B |         29.00 |        12.00 |           12.00 |        12.00 |        12.00 | ok     |
| [Q26](benchmarks/queries/query_26.sql) |        0 B |         68.00 |        16.00 |           16.00 |        16.00 |        16.00 | ok     |
| [Q27](benchmarks/queries/query_27.sql) |        0 B |        450.00 |       389.00 |          389.50 |       382.00 |       395.00 | ok     |
| [Q28](benchmarks/queries/query_28.sql) |        0 B |        929.00 |       875.50 |          873.00 |       865.00 |       891.00 | ok     |
| [Q29](benchmarks/queries/query_29.sql) |        0 B |         16.00 |         4.00 |            4.00 |         4.00 |         4.00 | ok     |
| [Q30](benchmarks/queries/query_30.sql) |        0 B |         72.00 |        24.00 |           24.00 |        24.00 |        24.00 | ok     |
| [Q31](benchmarks/queries/query_31.sql) |        0 B |        131.00 |        36.25 |           36.00 |        36.00 |        37.00 | ok     |
| [Q32](benchmarks/queries/query_32.sql) |        0 B |        484.00 |       398.75 |          396.50 |       383.00 |       419.00 | ok     |
| [Q33](benchmarks/queries/query_33.sql) |        0 B |        253.00 |       196.75 |          195.50 |       186.00 |       210.00 | ok     |
| [Q34](benchmarks/queries/query_34.sql) |        0 B |        248.00 |       188.25 |          189.00 |       184.00 |       191.00 | ok     |
| [Q35](benchmarks/queries/query_35.sql) |        0 B |         34.00 |        19.75 |           19.50 |        19.00 |        21.00 | ok     |
| [Q36](benchmarks/queries/query_36.sql) |        0 B |        315.00 |       273.25 |          272.50 |       271.00 |       277.00 | ok     |
| [Q37](benchmarks/queries/query_37.sql) |        0 B |        235.00 |       197.25 |          197.50 |       195.00 |       199.00 | ok     |
| [Q38](benchmarks/queries/query_38.sql) |        0 B |         86.00 |        45.75 |           46.00 |        45.00 |        46.00 | ok     |
| [Q39](benchmarks/queries/query_39.sql) |        0 B |        378.00 |       308.75 |          309.00 |       304.00 |       313.00 | ok     |
| [Q40](benchmarks/queries/query_40.sql) |        0 B |         76.00 |        38.75 |           38.50 |        38.00 |        40.00 | ok     |
| [Q41](benchmarks/queries/query_41.sql) |        0 B |         64.00 |        26.75 |           27.00 |        26.00 |        27.00 | ok     |
| [Q42](benchmarks/queries/query_42.sql) |        0 B |        100.00 |        70.25 |           70.00 |        70.00 |        71.00 | ok     |

<!-- benchmark-table:end -->
