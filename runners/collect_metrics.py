import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import psutil


@dataclass
class MetricsResult:
    return_code: int
    duration_s: float
    peak_rss_mb: float
    input_bytes: int
    throughput_mb_s: Optional[float]


def _sum_input_bytes(paths: Iterable[str]) -> int:
    total = 0
    for path in paths:
        if not path:
            continue
        try:
            total += Path(path).stat().st_size
        except FileNotFoundError:
            continue
    return total


def run_command_with_metrics(
    command: List[str],
    input_paths: Optional[List[str]] = None,
    sample_interval_s: float = 0.2,
) -> MetricsResult:
    input_bytes = _sum_input_bytes(input_paths or [])
    start = time.perf_counter()
    process = psutil.Popen(command)
    peak_rss = 0

    while process.is_running():
        try:
            rss = process.memory_info().rss
            for child in process.children(recursive=True):
                if child.is_running():
                    rss += child.memory_info().rss
            peak_rss = max(peak_rss, rss)
        except psutil.Error:
            pass
        time.sleep(sample_interval_s)

    return_code = process.wait()
    duration = max(time.perf_counter() - start, 1e-6)
    peak_rss_mb = peak_rss / (1024 * 1024)
    throughput_mb_s = None
    if input_bytes > 0:
        throughput_mb_s = (input_bytes / (1024 * 1024)) / duration

    return MetricsResult(
        return_code=return_code,
        duration_s=duration,
        peak_rss_mb=peak_rss_mb,
        input_bytes=input_bytes,
        throughput_mb_s=throughput_mb_s,
    )


def write_metrics(path: str, payload: dict) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def main() -> None:
    import argparse
    import shlex

    parser = argparse.ArgumentParser(description="Run a command and collect metrics.")
    parser.add_argument("--command", required=True, help="Command to execute")
    parser.add_argument("--output", required=True, help="Path to JSON output")
    parser.add_argument(
        "--input-path",
        action="append",
        default=[],
        help="Input file paths used to compute throughput",
    )
    args = parser.parse_args()

    command = shlex.split(args.command)
    result = run_command_with_metrics(command, args.input_path)
    write_metrics(args.output, {"metrics": asdict(result)})


if __name__ == "__main__":
    main()
