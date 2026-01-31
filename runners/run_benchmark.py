import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import List, Tuple

import yaml

from runners.collect_metrics import run_command_with_metrics
from runners.run_flowpipe import run_flowpipe
from runners.run_spark import build_command as build_spark_command


def resolve_benchmark_path(benchmark: str) -> Path:
    candidate = Path(benchmark)
    if candidate.exists():
        return candidate
    return Path("benchmarks") / f"{benchmark}.yaml"


def load_benchmark(benchmark_path: Path) -> dict:
    with benchmark_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def run_flowpipe_pipeline(pipeline: str, command_template: str) -> Tuple[List[str], List[str]]:
    return run_flowpipe(pipeline, command_template)


def run_spark_pipeline(args: argparse.Namespace, pipeline: str) -> Tuple[List[str], List[str]]:
    spark_args = argparse.Namespace(
        pipeline=pipeline,
        spark_submit=args.spark_submit,
        master=args.spark_master,
        input_base=args.input_base,
        output_base=args.output_base,
        input_ext=args.input_ext,
        tables=args.tables,
        lineitem=args.lineitem,
        part=args.part,
        output=args.output,
        format=args.format,
        delimiter=args.delimiter,
        dry_run=False,
    )
    return build_spark_command(spark_args)


def run_benchmark(args: argparse.Namespace) -> dict:
    benchmark_path = resolve_benchmark_path(args.benchmark)
    if not benchmark_path.exists():
        raise FileNotFoundError(f"Benchmark not found: {benchmark_path}")

    benchmark = load_benchmark(benchmark_path)
    pipeline = benchmark.get("pipeline")
    if not pipeline:
        raise ValueError("Benchmark is missing pipeline name")

    if args.engine == "flowpipe":
        command, input_paths = run_flowpipe_pipeline(pipeline, args.flowpipe_command_template)
    else:
        command, input_paths = run_spark_pipeline(args, pipeline)

    warmup_runs = benchmark.get("runs", {}).get("warmup", 0)
    measured_runs = benchmark.get("runs", {}).get("measured", 1)

    results = []
    for _ in range(warmup_runs):
        run_command_with_metrics(command, input_paths)

    for _ in range(measured_runs):
        metrics = run_command_with_metrics(command, input_paths)
        results.append(asdict(metrics))

    return {
        "benchmark": benchmark.get("name", benchmark_path.stem),
        "engine": args.engine,
        "pipeline": pipeline,
        "command": command,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a benchmark")
    parser.add_argument("--benchmark", required=True, help="Benchmark name or path")
    parser.add_argument("--engine", choices=["flowpipe", "spark"], required=True)
    parser.add_argument("--results-output", help="Output JSON path")

    parser.add_argument(
        "--flowpipe-command-template",
        default="flow-pipe",
        help="Command template for Flow-Pipe (supports {pipeline})",
    )

    parser.add_argument("--spark-submit")
    parser.add_argument("--spark-master")

    parser.add_argument("--input-base")
    parser.add_argument("--output-base")
    parser.add_argument("--input-ext")
    parser.add_argument("--tables")

    parser.add_argument("--lineitem")
    parser.add_argument("--part")
    parser.add_argument("--output")
    parser.add_argument("--format")
    parser.add_argument("--delimiter")
    args = parser.parse_args()

    payload = run_benchmark(args)
    output_path = (
        Path(args.results_output)
        if args.results_output
        else Path("results") / f"{payload['benchmark']}-{payload['engine']}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


if __name__ == "__main__":
    main()
