# Flow-Pipe Benchmarks

This repository contains benchmark definitions and runnable pipelines used to compare Flow-Pipe and Apache Spark on a shared workload suite. It includes TPC-H query workloads and a batch ETL conversion pipeline, plus scripts to generate datasets and collect run-time metrics.

## Repository layout

- `benchmarks/`: Benchmark definitions (workload, dataset, warmup + measured runs).
- `pipelines/flowpipe/`: Flow-Pipe pipeline specs for TPC-H queries and ETL.
- `pipelines/pyspark/`: PySpark pipeline implementations of the same workloads.
- `runners/`: CLI utilities for building commands and collecting metrics.
- `datasets/`: Dataset manifest and guidance for data generation.
- `schemas/`: Dataset schemas (TPC-H).
- `configs/`: Hardware/engine configuration templates.
- `docs/`: Methodology, reproduction steps, and policies.
- `scripts/`: Helpers for generating and converting TPC-H data.
- `docker/`: Container images for Flow-Pipe, Spark, and the benchmark runner.

## Quick start

1. **Prepare data** (TPC-H scale factor 10 by default):

   ```bash
   scripts/generate_tpch.sh --scale 10
   scripts/convert_tbl_to_csv.sh --input data/tpch/sf10 --output data/tpch/sf10
   ```

2. **Convert CSV to Parquet** (Spark example):

   ```bash
   python -m runners.run_spark --pipeline csv_to_parquet
   ```

3. **Run a benchmark** (example: TPC-H Q6 on Flow-Pipe):

   ```bash
   python -m runners.run_benchmark --benchmark tpch-q6 --engine flowpipe
   ```

4. **Inspect results** in `results/` (JSON with timing, peak RSS, and throughput).

For detailed guidance, see the documentation in `docs/`.

## Run benchmarks in Docker

The repository ships with container images for the benchmark runner, Flow-Pipe, and Spark. Build the runner image and execute benchmarks inside the container while using the bundled engine binaries.

1. **Build the benchmark runner image**:

   ```bash
   docker compose -f docker/docker-compose.yaml build runner
   ```

2. **Run a Flow-Pipe benchmark in Docker**:

   ```bash
   docker compose -f docker/docker-compose.yaml run --rm runner \
     --benchmark tpch-q6 \
     --engine flowpipe \
     --flowpipe-command-template /opt/flow-pipe/bin/flow-pipe
   ```

3. **Run a Spark benchmark in Docker**:

   ```bash
   docker compose -f docker/docker-compose.yaml run --rm runner \
     --benchmark tpch-q6 \
     --engine spark \
     --spark-submit /opt/spark/bin/spark-submit \
     --spark-master local[*]
   ```

The `docker-compose.yaml` mounts the repository into `/workspace` so datasets and results are stored on the host. Use the same data generation scripts before running the containerized benchmarks.

## Benchmarks included

- `batch-etl-csv-to-parquet`
- `tpch-q1`
- `tpch-q6`
- `tpch-q14`

Refer to `benchmarks/` for the full YAML definitions.
