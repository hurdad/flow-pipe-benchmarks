# Methodology

This benchmark suite measures end-to-end execution time, peak memory usage, and input throughput for a fixed set of pipelines. Results are captured in JSON to make comparison and aggregation straightforward.

## Workload definition

Each workload is defined in `benchmarks/*.yaml` with:

- A logical name
- The dataset to use
- The pipeline name
- Warmup + measured run counts

## Execution flow

1. Resolve the pipeline for the selected engine (Flow-Pipe or Spark).
2. Construct the engine-specific command.
3. Execute warmup runs without recording results.
4. Execute measured runs and record metrics.

## Metrics collected

Metrics are collected by `runners/collect_metrics.py`:

- **Duration** (`duration_s`): Wall-clock time for each run.
- **Peak RSS** (`peak_rss_mb`): Maximum resident set size across the process tree.
- **Input bytes** (`input_bytes`): Sum of input file sizes used for throughput estimation.
- **Throughput** (`throughput_mb_s`): Input bytes divided by duration.

These values are stored in the output JSON for each run.

## Results output

By default, results are written to `results/<benchmark>-<engine>.json`. You can override this path with `--results-output` when invoking the benchmark runner.
