#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: convert_tbl_to_csv.sh --input <tbl_dir> --output <csv_dir> [--input-delim <char>] [--output-delim <char>]

Converts TPC-H .tbl files (pipe-delimited with trailing delimiter) into delimited text files.

Options:
  --input         Directory containing .tbl files.
  --output        Directory where converted files will be written.
  --input-delim   Input delimiter character. Defaults to '|'.
  --output-delim  Output delimiter character. Defaults to ','.
  -h, --help      Show this help message.
USAGE
}

input_dir=""
output_dir=""
input_delim="|"
output_delim=","

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      input_dir="$2"
      shift 2
      ;;
    --output)
      output_dir="$2"
      shift 2
      ;;
    --input-delim)
      input_delim="$2"
      shift 2
      ;;
    --output-delim)
      output_delim="$2"
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

if [[ -z "$input_dir" || -z "$output_dir" ]]; then
  echo "--input and --output are required." >&2
  usage
  exit 1
fi

if [[ ! -d "$input_dir" ]]; then
  echo "Input directory not found: $input_dir" >&2
  exit 1
fi

if [[ ${#input_delim} -ne 1 || ${#output_delim} -ne 1 ]]; then
  echo "Input/output delimiters must be single characters." >&2
  exit 1
fi

mkdir -p "$output_dir"

shopt -s nullglob
files=("$input_dir"/*.tbl)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "No .tbl files found in $input_dir" >&2
  exit 1
fi

for tbl_file in "${files[@]}"; do
  base_name=$(basename "${tbl_file%.tbl}")
  output_file="$output_dir/${base_name}.csv"
  awk -v input_delim="$input_delim" -v output_delim="$output_delim" '
    BEGIN {
      escaped = input_delim
      gsub(/[][\\.^$*+?()|{}]/, "\\\\&", escaped)
      FS = escaped
      OFS = output_delim
    }
    {
      $1 = $1
      print
    }
  ' "$tbl_file" > "$output_file"
  echo "Converted $tbl_file -> $output_file"
 done
