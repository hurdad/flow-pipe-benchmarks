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

## Benchmarks included

- `batch-etl-csv-to-parquet`
- `tpch-q1`
- `tpch-q6`
- `tpch-q14`

Refer to `benchmarks/` for the full YAML definitions.
