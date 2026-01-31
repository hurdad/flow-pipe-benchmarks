import argparse
import os
import shlex
from pathlib import Path
from typing import List, Tuple

import yaml

SOURCE_TYPES = {
    "csv_arrow_source",
    "parquet_arrow_source",
    "orc_arrow_source",
    "json_arrow_source",
}


def resolve_pipeline_path(pipeline: str) -> Path:
    candidate = Path(pipeline)
    if candidate.exists():
        return candidate
    return Path("pipelines/flowpipe") / f"{pipeline}.yaml"


def extract_input_paths(pipeline_path: Path) -> List[str]:
    with pipeline_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    input_paths = []
    for stage in data.get("stages", []):
        if stage.get("type") in SOURCE_TYPES:
            path = stage.get("config", {}).get("path")
            if path:
                input_paths.append(path)
    return input_paths


def build_command(pipeline_path: Path, command_template: str) -> List[str]:
    if "{pipeline}" in command_template:
        return shlex.split(command_template.format(pipeline=pipeline_path))
    return shlex.split(command_template) + [str(pipeline_path)]


def run_flowpipe(pipeline: str, command_template: str) -> Tuple[List[str], List[str]]:
    pipeline_path = resolve_pipeline_path(pipeline)
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Pipeline not found: {pipeline_path}")
    command = build_command(pipeline_path, command_template)
    input_paths = extract_input_paths(pipeline_path)
    return command, input_paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Flow-Pipe pipeline")
    parser.add_argument("--pipeline", required=True, help="Pipeline name or path")
    parser.add_argument(
        "--command-template",
        default=os.getenv("FLOWPIPE_RUN_CMD", "flow-pipe"),
        help="Command template, supports {pipeline} placeholder",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print command and exit")
    args = parser.parse_args()

    command, _ = run_flowpipe(args.pipeline, args.command_template)
    if args.dry_run:
        print(" ".join(command))
        return

    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
