#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: generate_tpch.sh [--scale <factor>] [--chunks <count>] [--output <dir>] [--dbgen-dir <dir>]

Generates TPC-H .tbl files using tpch-dbgen and writes them to an output directory.

Options:
  --scale      Scale factor for data generation. Defaults to 10.
  --chunks     Number of chunks to generate. Defaults to 1 (no chunking).
  --output     Output directory for .tbl files. Defaults to data/tpch/sf<scale>.
  --dbgen-dir  Directory containing the tpch-dbgen source/build. Defaults to third_party/tpch-dbgen.
  -h, --help   Show this help message.
USAGE
}

scale_factor="10"
chunks="1"
output_dir=""
dbgen_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale)
      scale_factor="$2"
      shift 2
      ;;
    --output)
      output_dir="$2"
      shift 2
      ;;
    --chunks)
      chunks="$2"
      shift 2
      ;;
    --dbgen-dir)
      dbgen_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
 done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)

if [[ -z "$output_dir" ]]; then
  output_dir="$repo_root/data/tpch/sf${scale_factor}"
fi

if [[ -z "$dbgen_dir" ]]; then
  dbgen_dir="$repo_root/third_party/tpch-dbgen"
fi

if ! [[ "$chunks" =~ ^[0-9]+$ ]] || [[ "$chunks" -lt 1 ]]; then
  echo "Chunks must be a positive integer (got: $chunks)" >&2
  exit 1
fi

mkdir -p "$output_dir"

if [[ ! -x "$dbgen_dir/dbgen" ]]; then
  if [[ ! -d "$dbgen_dir" ]]; then
    echo "Cloning tpch-dbgen into $dbgen_dir"
    git clone https://github.com/electrum/tpch-dbgen.git "$dbgen_dir"
  fi

  echo "Building tpch-dbgen"
  (cd "$dbgen_dir" && make)
fi

if [[ ! -x "$dbgen_dir/dbgen" ]]; then
  echo "dbgen not found after build: $dbgen_dir/dbgen" >&2
  exit 1
fi

tmp_dir=$(mktemp -d)
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

generate_chunk() {
  local chunk_index="$1"
  local chunk_output_dir="$output_dir"

  if [[ "$chunks" -gt 1 ]]; then
    chunk_output_dir="$output_dir/chunk-${chunk_index}"
    mkdir -p "$chunk_output_dir"
  fi

  (
     cd "$tmp_dir"
     rm -f ./*.tbl
     cp "$dbgen_dir/dists.dss" .
     if [[ "$chunks" -gt 1 ]]; then
       "$dbgen_dir/dbgen" -s "$scale_factor" -f -C "$chunks" -S "$chunk_index"
     else
       "$dbgen_dir/dbgen" -s "$scale_factor" -f
     fi
  )

  shopt -s nullglob
  files=("$tmp_dir"/*.tbl)
  if [[ ${#files[@]} -eq 0 ]]; then
    echo "No .tbl files generated." >&2
    exit 1
  fi

  for tbl_file in "${files[@]}"; do
    chmod 777 "$tbl_file"
    mv "$tbl_file" "$chunk_output_dir/"
    echo "Wrote $(basename "$tbl_file") to $chunk_output_dir"
  done
}

for ((chunk_index=1; chunk_index<=chunks; chunk_index++)); do
  generate_chunk "$chunk_index"
done

if [[ "$chunks" -gt 1 ]]; then
  cat <<EOF
TPC-H data generated at: $output_dir (chunked into chunk-<n> subdirectories)
To convert to CSV per chunk:
  $script_dir/convert_tbl_to_csv.sh --input "$output_dir/chunk-1" --output "$output_dir/chunk-1"
EOF
else
  cat <<EOF
TPC-H data generated at: $output_dir
To convert to CSV:
  $script_dir/convert_tbl_to_csv.sh --input "$output_dir" --output "$output_dir"
EOF
fi
