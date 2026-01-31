import argparse
import os
import shlex
from pathlib import Path
from typing import Dict, List, Tuple


def resolve_pipeline_path(pipeline: str) -> Path:
    candidate = Path(pipeline)
    if candidate.exists():
        return candidate
    return Path("pipelines/pyspark") / f"{pipeline}.py"


def _default_paths() -> Dict[str, str]:
    return {
        "csv_base": "data/tpch/sf10",
        "parquet_base": "data/tpch/sf10-parquet",
        "results_base": "data/tpch/sf10-results",
    }


def build_command(args: argparse.Namespace) -> Tuple[List[str], List[str]]:
    pipeline_path = resolve_pipeline_path(args.pipeline)
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Pipeline not found: {pipeline_path}")

    defaults = _default_paths()
    pipeline_name = pipeline_path.stem

    spark_submit = args.spark_submit or os.getenv("SPARK_SUBMIT_CMD", "spark-submit")
    command = shlex.split(spark_submit)
    if args.master:
        command += ["--master", args.master]
    command.append(str(pipeline_path))

    input_paths: List[str] = []

    if pipeline_name == "csv_to_parquet":
        input_base = args.input_base or defaults["csv_base"]
        output_base = args.output_base or defaults["parquet_base"]
        tables = args.tables or "lineitem,orders,part,partsupp,customer,supplier,nation,region"
        input_ext = args.input_ext or "csv"
        delimiter = args.delimiter or "|"
        command += [
            "--input-base",
            input_base,
            "--output-base",
            output_base,
            "--input-ext",
            input_ext,
            "--delimiter",
            delimiter,
            "--tables",
            tables,
        ]
        for table in tables.split(","):
            table = table.strip()
            if table:
                input_paths.append(f"{input_base}/{table}.{input_ext}")
    elif pipeline_name == "tpch_q1":
        lineitem = args.lineitem or f"{defaults['parquet_base']}/lineitem"
        output = args.output or f"{defaults['results_base']}/tpch_q1"
        fmt = args.format or "parquet"
        delimiter = args.delimiter or "|"
        command += [
            "--input",
            lineitem,
            "--format",
            fmt,
            "--delimiter",
            delimiter,
            "--output",
            output,
        ]
        input_paths.append(lineitem)
    elif pipeline_name == "tpch_q6":
        lineitem = args.lineitem or f"{defaults['parquet_base']}/lineitem"
        output = args.output or f"{defaults['results_base']}/tpch_q6"
        fmt = args.format or "parquet"
        delimiter = args.delimiter or "|"
        command += [
            "--input",
            lineitem,
            "--format",
            fmt,
            "--delimiter",
            delimiter,
            "--output",
            output,
        ]
        input_paths.append(lineitem)
    elif pipeline_name == "tpch_q14":
        lineitem = args.lineitem or f"{defaults['parquet_base']}/lineitem"
        part = args.part or f"{defaults['parquet_base']}/part"
        output = args.output or f"{defaults['results_base']}/tpch_q14"
        fmt = args.format or "parquet"
        delimiter = args.delimiter or "|"
        command += [
            "--lineitem",
            lineitem,
            "--part",
            part,
            "--format",
            fmt,
            "--delimiter",
            delimiter,
            "--output",
            output,
        ]
        input_paths.extend([lineitem, part])
    else:
        raise ValueError(f"Unsupported pipeline: {pipeline_name}")

    return command, input_paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Spark pipeline")
    parser.add_argument("--pipeline", required=True, help="Pipeline name or path")
    parser.add_argument("--spark-submit", help="spark-submit command")
    parser.add_argument("--master", help="Spark master URL")

    parser.add_argument("--input-base")
    parser.add_argument("--output-base")
    parser.add_argument("--input-ext")
    parser.add_argument("--tables")

    parser.add_argument("--lineitem")
    parser.add_argument("--part")
    parser.add_argument("--output")
    parser.add_argument("--format")
    parser.add_argument("--delimiter")

    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    command, _ = build_command(args)
    if args.dry_run:
        print(" ".join(command))
        return

    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
