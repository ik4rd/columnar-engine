#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import datetime as dt
import pathlib
import statistics
import subprocess
import sys
import tempfile
import time

README_START = "<!-- benchmark-table:start -->"
README_END = "<!-- benchmark-table:end -->"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the first N benchmark queries, write CSV stats, and refresh the README benchmark table."
    )
    parser.add_argument("--count", type=int, default=17, help="Number of first queries to benchmark.")
    parser.add_argument("--warm-runs", type=int, default=4, help="Number of warm runs after the first measured run.")
    parser.add_argument(
        "--query-dir",
        type=pathlib.Path,
        default=pathlib.Path("benchmarks/queries"),
        help="Directory with query_<id>.sql files.",
    )
    parser.add_argument(
        "--input",
        type=pathlib.Path,
        default=pathlib.Path("benchmarks/hits_sample.columnar"),
        help="Columnar input file.",
    )
    parser.add_argument(
        "--binary",
        type=pathlib.Path,
        default=pathlib.Path("build/src/columnar_engine"),
        help="Path to the columnar_engine binary.",
    )
    parser.add_argument(
        "--csv-out",
        type=pathlib.Path,
        default=pathlib.Path("benchmarks/results/readme_benchmarks.csv"),
        help="Where to write the benchmark CSV.",
    )
    parser.add_argument(
        "--readme",
        type=pathlib.Path,
        default=pathlib.Path("README.md"),
        help="README file to update.",
    )
    parser.add_argument(
        "--table-name",
        default="hits",
        help="Logical table name passed to run-query.",
    )
    return parser.parse_args()


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent


def resolve_path(path: pathlib.Path, root: pathlib.Path) -> pathlib.Path:
    return path if path.is_absolute() else root / path


def format_ms(value: float) -> str:
    return f"{value:.2f}"


def compact_sql(sql: str, max_len: int = 88) -> str:
    normalized = " ".join(sql.split())
    if len(normalized) <= max_len:
        return normalized
    return normalized[: max_len - 1] + "…"


def markdown_escape(text: str) -> str:
    return text.replace("|", "\\|")


def run_query(
        binary: pathlib.Path,
        input_path: pathlib.Path,
        query_path: pathlib.Path,
        output_path: pathlib.Path,
        table_name: str,
) -> float:
    command = [
        str(binary),
        "run-query",
        "--input",
        str(input_path),
        "--output",
        str(output_path),
        "--table-name",
        table_name,
        "--query-file",
        str(query_path),
    ]

    started_at = time.perf_counter()
    completed = subprocess.run(command, capture_output=True, text=True)
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0

    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        message = stderr or stdout or f"exit code {completed.returncode}"
        raise RuntimeError(message)

    return elapsed_ms


def collect_rows(
        *,
        root: pathlib.Path,
        binary: pathlib.Path,
        input_path: pathlib.Path,
        query_dir: pathlib.Path,
        count: int,
        warm_runs: int,
        table_name: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    with tempfile.TemporaryDirectory(prefix="readme_benchmarks_") as temp_dir_name:
        temp_dir = pathlib.Path(temp_dir_name)

        for query_id in range(count):
            query_path = query_dir / f"query_{query_id}.sql"
            if not query_path.exists():
                break

            sql = query_path.read_text(encoding="utf-8").strip()
            output_path = temp_dir / f"query_{query_id}.csv"

            first_error = ""
            status = "ok"
            first_run_ms = ""
            warm_avg_ms = ""
            warm_min_ms = ""
            warm_max_ms = ""
            warm_median_ms = ""

            try:
                first_run_value = run_query(binary, input_path, query_path, output_path, table_name)
                first_run_ms = format_ms(first_run_value)

                warm_values: list[float] = []
                for _ in range(warm_runs):
                    warm_values.append(run_query(binary, input_path, query_path, output_path, table_name))

                if warm_values:
                    warm_avg_ms = format_ms(sum(warm_values) / len(warm_values))
                    warm_min_ms = format_ms(min(warm_values))
                    warm_max_ms = format_ms(max(warm_values))
                    warm_median_ms = format_ms(statistics.median(warm_values))
            except RuntimeError as error:
                status = "failed"
                first_error = str(error)

            rows.append(
                {
                    "query_id": str(query_id),
                    "query_file": str(query_path.relative_to(root)),
                    "sql": sql,
                    "first_run_ms": first_run_ms,
                    "warm_avg_ms": warm_avg_ms,
                    "warm_median_ms": warm_median_ms,
                    "warm_min_ms": warm_min_ms,
                    "warm_max_ms": warm_max_ms,
                    "status": status,
                    "error": first_error,
                }
            )

    return rows


def write_csv(csv_path: pathlib.Path, rows: list[dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "query_id",
        "query_file",
        "sql",
        "first_run_ms",
        "warm_avg_ms",
        "warm_median_ms",
        "warm_min_ms",
        "warm_max_ms",
        "status",
        "error",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def render_markdown(csv_path: pathlib.Path, rows: list[dict[str, str]], count: int, warm_runs: int) -> str:
    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "## Dashboard",
        "",
        "",
        "| Query | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for row in rows:
        query_file = row["query_file"]
        query_label = f"[{row['query_id']}]({query_file})"
        status = row["status"]
        if row["error"]:
            status = f"{status}: {markdown_escape(compact_sql(row['error'], max_len=48))}"

        lines.append(
            "| "
            + " | ".join(
                [
                    query_label,
                    row["first_run_ms"] or "—",
                    row["warm_avg_ms"] or "—",
                    row["warm_median_ms"] or "—",
                    row["warm_min_ms"] or "—",
                    row["warm_max_ms"] or "—",
                    status,
                ]
            )
            + " |"
        )

    return "\n".join(lines)


def update_readme(readme_path: pathlib.Path, markdown_block: str) -> None:
    if readme_path.exists():
        content = readme_path.read_text(encoding="utf-8")
    else:
        content = ""

    wrapped = f"{README_START}\n{markdown_block}\n{README_END}"

    if README_START in content and README_END in content:
        start = content.index(README_START)
        end = content.index(README_END) + len(README_END)
        updated = content[:start] + wrapped + content[end:]
    else:
        suffix = "" if not content or content.endswith("\n") else "\n"
        updated = content + suffix + "\n" + wrapped + "\n"

    readme_path.write_text(updated, encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.count <= 0:
        raise SystemExit("--count must be > 0")
    if args.warm_runs < 0:
        raise SystemExit("--warm-runs must be >= 0")

    root = repo_root()
    binary = resolve_path(args.binary, root)
    input_path = resolve_path(args.input, root)
    query_dir = resolve_path(args.query_dir, root)
    csv_path = resolve_path(args.csv_out, root)
    readme_path = resolve_path(args.readme, root)

    if not binary.exists():
        raise SystemExit(f"binary not found: {binary}")
    if not input_path.exists():
        raise SystemExit(f"input file not found: {input_path}")
    if not query_dir.exists():
        raise SystemExit(f"query directory not found: {query_dir}")

    rows = collect_rows(
        root=root,
        binary=binary,
        input_path=input_path,
        query_dir=query_dir,
        count=args.count,
        warm_runs=args.warm_runs,
        table_name=args.table_name,
    )
    write_csv(csv_path, rows)
    markdown_block = render_markdown(csv_path.relative_to(root), rows, args.count, args.warm_runs)
    update_readme(readme_path, markdown_block)
    print(f"updated {readme_path.relative_to(root)} from {csv_path.relative_to(root)} with {len(rows)} row(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
