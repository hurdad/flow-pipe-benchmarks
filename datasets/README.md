# Datasets

This folder documents the datasets used by the benchmark suite. Dataset metadata lives in `datasets/manifest.yaml`, including the logical name, expected format, and source location.

## TPC-H (sf10)

The default dataset is `tpch-sf10`, generated via `tpch-dbgen` at scale factor 10. Use the helper scripts to generate and convert the data locally:

```bash
scripts/generate_tpch.sh --scale 10
scripts/convert_tbl_to_csv.sh --input data/tpch/sf10 --output data/tpch/sf10
```

The conversion script removes the trailing delimiter from `.tbl` files and writes CSV output.

## Updating dataset locations

Benchmark pipelines assume the following default paths:

- CSV: `data/tpch/sf10`
- Parquet: `data/tpch/sf10-parquet`
- Results: `data/tpch/sf10-results`

If you store data elsewhere, update the pipeline configuration or supply overrides when invoking the runner (`--input-base`, `--output-base`, `--lineitem`, etc.).
