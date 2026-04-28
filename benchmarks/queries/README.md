Place benchmark SQL files here as `query_<id>.sql`.

Example:

- `benchmarks/queries/query_0.sql`
- `benchmarks/queries/query_1.sql`

`./script/run_query.sh` resolves the benchmark query number to the matching SQL file and executes it through:

`columnar_engine run-query --input <table.columnar> --query-file <query.sql> --output <result.csv>`

This keeps benchmark queries editable as plain SQL instead of hardcoding them in shell scripts or C++.
