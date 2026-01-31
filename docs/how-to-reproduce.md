# How to Reproduce

This guide walks through generating data, running pipelines, and capturing benchmark results.

## Prerequisites

- Python 3.9+ with `psutil` and `pyyaml` available.
- A Flow-Pipe binary on your `PATH` (for Flow-Pipe runs).
- Apache Spark (for Spark runs).
- Alternatively, Docker + Docker Compose (for containerized runs).

## 1) Generate TPC-H data

```bash
scripts/generate_tpch.sh --scale 10
scripts/convert_tbl_to_csv.sh --input data/tpch/sf10 --output data/tpch/sf10
```

## 2) Convert CSV to Parquet (optional but recommended)

Spark pipeline:

```bash
python -m runners.run_spark --pipeline csv_to_parquet
```

Flow-Pipe pipeline:

```bash
python -m runners.run_flowpipe --pipeline csv_to_parquet
```

## 3) Run benchmarks

Flow-Pipe example:

```bash
python -m runners.run_benchmark --benchmark tpch-q6 --engine flowpipe
```

Spark example:

```bash
python -m runners.run_benchmark --benchmark tpch-q6 --engine spark --spark-submit spark-submit --spark-master local[*]
```

### Running benchmarks in Docker

If you prefer containerized runs, build the runner image and execute benchmarks using the binaries baked into the container.

```bash
docker compose -f docker/docker-compose.yaml build runner
```

Flow-Pipe in Docker:

```bash
docker compose -f docker/docker-compose.yaml run --rm runner \
  --benchmark tpch-q6 \
  --engine flowpipe \
  --flowpipe-command-template /opt/flow-pipe/bin/flow-pipe
```

Spark in Docker:

```bash
docker compose -f docker/docker-compose.yaml run --rm runner \
  --benchmark tpch-q6 \
  --engine spark \
  --spark-submit /opt/spark/bin/spark-submit \
  --spark-master local[*]
```

The compose file mounts the repository into `/workspace` so input data and result JSONs are stored on the host filesystem.

## 4) Review results

Result JSON files are written to `results/` by default. Each file includes command metadata and per-run metrics. Adjust `--results-output` to direct output elsewhere.

## 5) Record environment details

Capture hardware configuration (see `docs/hardware-setup.md`) and any configuration overrides used during the run to ensure results are comparable.
