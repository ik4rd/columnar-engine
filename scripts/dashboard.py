#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import pathlib
import platform
import shlex
import shutil
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
    parser.add_argument("--count", type=int, default=43, help="Number of first queries to benchmark.")
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
    parser.add_argument(
        "--source-csv",
        type=pathlib.Path,
        default=pathlib.Path("benchmarks/hits_sample.csv"),
        help="Source CSV used to build the benchmark columnar file.",
    )
    parser.add_argument(
        "--schema",
        type=pathlib.Path,
        default=pathlib.Path("benchmarks/scheme.csv"),
        help="Schema CSV used for conversion benchmarks.",
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        default=1 << 14,
        help="Row group size for the CSV -> columnar conversion benchmark.",
    )
    parser.add_argument(
        "--compression",
        default="none",
        choices=("none", "lz4"),
        help="Compression codec for the CSV -> columnar conversion benchmark.",
    )
    parser.add_argument(
        "--cache-drop-mode",
        default="per-query",
        choices=("none", "once", "per-query"),
        help=(
            "Drop the filesystem page cache before benchmarks: never, once before all queries, "
            "or before the first run of each query."
        ),
    )
    parser.add_argument(
        "--cache-drop-command",
        help="Override the OS-specific cache drop command, for example: 'sudo purge'.",
    )
    return parser.parse_args()


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent


def resolve_path(path: pathlib.Path, root: pathlib.Path) -> pathlib.Path:
    return path if path.is_absolute() else root / path


def format_ms(value: float) -> str:
    return f"{value:.2f}"


def format_seconds(value: float) -> str:
    return f"{value:.2f}s"


def format_ratio(value: float) -> str:
    return f"{value:.2f}x"


def format_percent(value: float) -> str:
    return f"{value:.1f}%"


def format_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"

    units = ["KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        value /= 1024.0
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f} {unit}"

    return f"{num_bytes} B"


def compact_sql(sql: str, max_len: int = 88) -> str:
    normalized = " ".join(sql.split())
    if len(normalized) <= max_len:
        return normalized
    return normalized[: max_len - 1] + "…"


def markdown_escape(text: str) -> str:
    return text.replace("|", "\\|")


def html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def render_html_metric_table(title: str, rows: list[tuple[str, str]]) -> list[str]:
    lines = [
        f"<h3>{html_escape(title)}</h3>",
        '<table>',
        '<thead><tr><th align="left">Metric</th><th align="right">Value</th></tr></thead>',
        "<tbody>",
    ]
    for metric, value in rows:
        lines.append(
            f"<tr><td>{html_escape(str(metric))}</td><td align=\"right\">{html_escape(str(value))}</td></tr>"
        )
    lines.extend(["</tbody>", "</table>"])
    return lines


def parse_ms(raw: str) -> float | None:
    if not raw:
        return None
    return float(raw)


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        raise ValueError("percentile() requires at least one value")

    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    position = ratio * (len(ordered) - 1)
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return ordered[lower_index]

    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    fraction = position - lower_index
    return lower_value + (upper_value - lower_value) * fraction


def collect_conversion_stats(
        *,
        root: pathlib.Path,
        binary: pathlib.Path,
        schema_path: pathlib.Path,
        source_csv_path: pathlib.Path,
        row_group_size: int,
        compression: str,
        drop_cache: bool,
        cache_drop_command: str | None,
) -> dict[str, float | int | str] | None:
    if not schema_path.exists() or not source_csv_path.exists():
        return None

    with tempfile.TemporaryDirectory(prefix="readme_conversion_") as temp_dir_name:
        temp_dir = pathlib.Path(temp_dir_name)
        output_columnar = temp_dir / "dashboard.columnar"
        roundtrip_schema = temp_dir / "dashboard_schema.csv"
        roundtrip_csv = temp_dir / "dashboard_roundtrip.csv"

        convert_command = [
            str(binary),
            "convert",
            "--schema",
            str(schema_path),
            "--input",
            str(source_csv_path),
            "--output",
            str(output_columnar),
            "--row-group-size",
            str(row_group_size),
            "--compression",
            compression,
        ]
        roundtrip_command = [
            str(binary),
            "roundtrip",
            "--input",
            str(output_columnar),
            "--schema-output",
            str(roundtrip_schema),
            "--csv-output",
            str(roundtrip_csv),
        ]

        if drop_cache:
            drop_file_system_cache(cache_drop_command)

        convert_started_at = time.perf_counter()
        convert_completed = subprocess.run(convert_command, capture_output=True, text=True)
        convert_elapsed_seconds = time.perf_counter() - convert_started_at
        if convert_completed.returncode != 0:
            message = convert_completed.stderr.strip() or convert_completed.stdout.strip() or "convert failed"
            raise RuntimeError(message)

        if drop_cache:
            drop_file_system_cache(cache_drop_command)

        roundtrip_started_at = time.perf_counter()
        roundtrip_completed = subprocess.run(roundtrip_command, capture_output=True, text=True)
        roundtrip_elapsed_seconds = time.perf_counter() - roundtrip_started_at
        if roundtrip_completed.returncode != 0:
            message = roundtrip_completed.stderr.strip() or roundtrip_completed.stdout.strip() or "roundtrip failed"
            raise RuntimeError(message)

        source_csv_bytes = source_csv_path.stat().st_size
        columnar_bytes = output_columnar.stat().st_size
        roundtrip_csv_bytes = roundtrip_csv.stat().st_size

    return {
        "source_csv_path": str(source_csv_path.relative_to(root)),
        "schema_path": str(schema_path.relative_to(root)),
        "compression": compression,
        "source_csv_bytes": source_csv_bytes,
        "columnar_bytes": columnar_bytes,
        "roundtrip_csv_bytes": roundtrip_csv_bytes,
        "convert_elapsed_seconds": convert_elapsed_seconds,
        "roundtrip_elapsed_seconds": roundtrip_elapsed_seconds,
        "compression_ratio": source_csv_bytes / columnar_bytes if columnar_bytes else 0.0,
        "columnar_vs_csv_percent": (columnar_bytes / source_csv_bytes * 100.0) if source_csv_bytes else 0.0,
        "convert_throughput_mb_s": (source_csv_bytes / (1024 * 1024)) / convert_elapsed_seconds
        if convert_elapsed_seconds
        else 0.0,
        "roundtrip_throughput_mb_s": (columnar_bytes / (1024 * 1024)) / roundtrip_elapsed_seconds
        if roundtrip_elapsed_seconds
        else 0.0,
    }


def default_cache_drop_command() -> list[str] | None:
    system = platform.system()
    if system == "Darwin":
        purge = shutil.which("purge")
        return [purge] if purge else None

    if system == "Linux":
        return ["__linux_drop_caches__"]

    return None


def run_cache_drop_command(command: list[str]) -> None:
    if not command:
        raise RuntimeError("cache drop command is empty")

    if command == ["__linux_drop_caches__"]:
        subprocess.run(["sync"], check=True)
        drop_caches_path = pathlib.Path("/proc/sys/vm/drop_caches")
        if os.geteuid() == 0:
            drop_caches_path.write_text("3\n", encoding="utf-8")
            return

        try:
            completed = subprocess.run(
                ["sudo", "-n", "tee", str(drop_caches_path)],
                input="3\n",
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as error:
            raise RuntimeError("cache drop requires root or sudo on Linux") from error
    else:
        try:
            completed = subprocess.run(command, capture_output=True, text=True)
        except FileNotFoundError as error:
            raise RuntimeError(f"cache drop command not found: {command[0]}") from error

    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        message = stderr or stdout or f"exit code {completed.returncode}"
        raise RuntimeError(message)


def drop_file_system_cache(cache_drop_command: str | None) -> None:
    command = shlex.split(cache_drop_command) if cache_drop_command else default_cache_drop_command()
    if command is None:
        raise RuntimeError(
            "cache drop is not supported on this OS; pass --cache-drop-mode none or --cache-drop-command"
        )

    run_cache_drop_command(command)


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
        cache_drop_mode: str,
        cache_drop_command: str | None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    with tempfile.TemporaryDirectory(prefix="readme_benchmarks_") as temp_dir_name:
        temp_dir = pathlib.Path(temp_dir_name)

        if cache_drop_mode == "once":
            drop_file_system_cache(cache_drop_command)

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
            output_csv_bytes = "0"

            if cache_drop_mode == "per-query":
                drop_file_system_cache(cache_drop_command)

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

                if output_path.exists():
                    output_csv_bytes = str(output_path.stat().st_size)
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
                    "output_csv_bytes": output_csv_bytes,
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
        "output_csv_bytes",
        "status",
        "error",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def query_metric(row: dict[str, str]) -> float:
    warm_median = parse_ms(row["warm_median_ms"])
    if warm_median is not None:
        return warm_median

    first_run = parse_ms(row["first_run_ms"])
    return first_run or 0.0


def heatmap_icon(value: float, median_value: float) -> str:
    if value <= 0:
        return "⬜"
    if value < median_value * 0.9:
        return "🟩"
    if value > median_value * 1.1:
        return "🟥"
    return "🟦"


def render_heatmap(rows: list[dict[str, str]], median_value: float, columns: int = 6) -> list[str]:
    lines = [
        "### Heatmap",
        "",
        "`🟩` быстрее медианы · `🟦` около медианы · `🟥` медленнее медианы · `⬜` 0 ms",
        "",
    ]

    chunks = [rows[index: index + columns] for index in range(0, len(rows), columns)]
    lines.append("| " + " | ".join(f"slot {index + 1}" for index in range(columns)) + " |")
    lines.append("| " + " | ".join("---:" for _ in range(columns)) + " |")
    for chunk in chunks:
        padded_chunk = chunk + [None] * (columns - len(chunk))
        lines.append(
            "| "
            + " | ".join(
                (
                    f"Q{int(row['query_id']):02d} {heatmap_icon(query_metric(row), median_value)} "
                    f"`{format_ms(query_metric(row))}`"
                )
                if row is not None
                else ""
                for row in padded_chunk
            )
            + " |"
        )

    return lines


def render_markdown(
        csv_path: pathlib.Path,
        rows: list[dict[str, str]],
        count: int,
        warm_runs: int,
        cache_drop_mode: str,
        conversion_stats: dict[str, float | int | str] | None,
) -> str:
    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ok_rows = [row for row in rows if row["status"] == "ok"]
    warm_values = [query_metric(row) for row in ok_rows if query_metric(row) > 0]
    first_run_values = [parse_ms(row["first_run_ms"]) for row in ok_rows if parse_ms(row["first_run_ms"]) is not None]
    first_run_values = [value for value in first_run_values if value is not None]
    failed_rows = [row for row in rows if row["status"] != "ok"]
    output_sizes = [int(row["output_csv_bytes"]) for row in ok_rows]
    median_value = statistics.median(warm_values) if warm_values else 0.0
    avg_value = statistics.fmean(warm_values) if warm_values else 0.0
    p95_value = percentile(warm_values, 0.95) if warm_values else 0.0
    cold_penalty = ((statistics.fmean(first_run_values) / avg_value) - 1.0) * 100.0 if avg_value and first_run_values else 0.0
    fastest = min(ok_rows, key=query_metric) if ok_rows else None
    slowest = max(ok_rows, key=query_metric) if ok_rows else None
    summary_rows = [
        ("queries ok", f"{len(ok_rows)} / {len(rows)}"),
        ("queries failed", str(len(failed_rows))),
        ("median warm time", f"{format_ms(median_value)} ms"),
        ("average warm time", f"{format_ms(avg_value)} ms"),
        ("p95 warm time", f"{format_ms(p95_value)} ms"),
        ("cold/warm delta", format_percent(cold_penalty)),
        ("total output size", format_size(sum(output_sizes))),
        ("max output size", format_size(max(output_sizes)) if output_sizes else "—"),
        (
            "fastest query",
            f"Q{int(fastest['query_id']):02d} · {format_ms(query_metric(fastest))} ms" if fastest else "—",
        ),
        (
            "slowest query",
            f"Q{int(slowest['query_id']):02d} · {format_ms(query_metric(slowest))} ms" if slowest else "—",
        ),
    ]

    lines = [
        "## Benchmark Dashboard",
        "",
        f"`generated {generated_at}` · `queries {len(rows)}/{count}` · `warm runs {warm_runs}` · "
        f"`cache drop {cache_drop_mode}` · [`csv`]({csv_path})",
        "",
    ]

    if conversion_stats is not None:
        storage_rows = [
            ("source csv", conversion_stats["source_csv_path"]),
            ("schema", conversion_stats["schema_path"]),
            ("compression", conversion_stats["compression"]),
            ("source size", format_size(int(conversion_stats["source_csv_bytes"]))),
            ("columnar size", format_size(int(conversion_stats["columnar_bytes"]))),
            ("roundtrip csv size", format_size(int(conversion_stats["roundtrip_csv_bytes"]))),
            ("compression ratio", format_ratio(float(conversion_stats["compression_ratio"]))),
            ("columnar / csv", format_percent(float(conversion_stats["columnar_vs_csv_percent"]))),
            ("csv -> columnar", format_seconds(float(conversion_stats["convert_elapsed_seconds"]))),
            ("columnar -> csv", format_seconds(float(conversion_stats["roundtrip_elapsed_seconds"]))),
            ("convert throughput", f"{float(conversion_stats['convert_throughput_mb_s']):.1f} MB/s"),
            ("roundtrip throughput", f"{float(conversion_stats['roundtrip_throughput_mb_s']):.1f} MB/s"),
        ]
        lines.extend(
            [
                '<table>',
                '<tr>',
                '<td valign="top" width="50%">',
                *render_html_metric_table("Summary", summary_rows),
                "</td>",
                '<td valign="top" width="50%">',
                *render_html_metric_table("Storage", storage_rows),
                "</td>",
                "</tr>",
                "</table>",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "### Summary",
                "",
                "| Metric | Value |",
                "| --- | ---: |",
                *[f"| {markdown_escape(metric)} | {markdown_escape(str(value))} |" for metric, value in summary_rows],
                "",
            ]
        )

    lines.extend(render_heatmap(rows, median_value))

    lines.extend(
        [
            "",
            "### Query Table",
            "",
            "| Query | Output CSV | First run, ms | Warm avg, ms | Warm median, ms | Warm min, ms | Warm max, ms | Status |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )

    for row in rows:
        query_file = row["query_file"]
        query_label = f"[Q{int(row['query_id']):02d}]({query_file})"
        status = row["status"]
        if row["error"]:
            status = f"{status}: {markdown_escape(compact_sql(row['error'], max_len=48))}"

        lines.append(
            "| "
            + " | ".join(
                [
                    query_label,
                    format_size(int(row["output_csv_bytes"])),
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
    source_csv_path = resolve_path(args.source_csv, root)
    schema_path = resolve_path(args.schema, root)

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
        cache_drop_mode=args.cache_drop_mode,
        cache_drop_command=args.cache_drop_command,
    )
    conversion_stats = collect_conversion_stats(
        root=root,
        binary=binary,
        schema_path=schema_path,
        source_csv_path=source_csv_path,
        row_group_size=args.row_group_size,
        compression=args.compression,
        drop_cache=args.cache_drop_mode != "none",
        cache_drop_command=args.cache_drop_command,
    )
    write_csv(csv_path, rows)
    markdown_block = render_markdown(
        csv_path.relative_to(root),
        rows,
        args.count,
        args.warm_runs,
        args.cache_drop_mode,
        conversion_stats,
    )
    update_readme(readme_path, markdown_block)
    print(f"updated {readme_path.relative_to(root)} from {csv_path.relative_to(root)} with {len(rows)} row(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
