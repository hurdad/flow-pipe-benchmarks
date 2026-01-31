import argparse
import sys
from dataclasses import dataclass
from functools import reduce
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as abs_, col, lit
from pyspark.sql.types import NumericType


@dataclass(frozen=True)
class PipelineValidationConfig:
    flowpipe_output: str
    spark_output: str
    key_columns: List[str]


PIPELINE_CONFIGS: Dict[str, PipelineValidationConfig] = {
    "tpch_q1": PipelineValidationConfig(
        flowpipe_output="data/tpch/sf10-results/tpch_q1.parquet",
        spark_output="data/tpch/sf10-results/tpch_q1",
        key_columns=["l_returnflag", "l_linestatus"],
    ),
    "tpch_q6": PipelineValidationConfig(
        flowpipe_output="data/tpch/sf10-results/tpch_q6.parquet",
        spark_output="data/tpch/sf10-results/tpch_q6",
        key_columns=[],
    ),
    "tpch_q14": PipelineValidationConfig(
        flowpipe_output="data/tpch/sf10-results/tpch_q14.parquet",
        spark_output="data/tpch/sf10-results/tpch_q14",
        key_columns=[],
    ),
}


def _prefixed_dataframe(df, prefix: str, key_columns: List[str]):
    for column in df.columns:
        if column not in key_columns:
            df = df.withColumnRenamed(column, f"{prefix}{column}")
    return df


def _numeric_columns(df) -> List[str]:
    return [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    ]


def _compare_single_row(
    flowpipe_df,
    spark_df,
    tolerance: float,
) -> bool:
    flowpipe_count = flowpipe_df.count()
    spark_count = spark_df.count()
    if flowpipe_count != 1 or spark_count != 1:
        print(
            "Expected exactly one row in each result set but found "
            f"flowpipe={flowpipe_count}, spark={spark_count}."
        )
        return False

    flowpipe_row = flowpipe_df.collect()[0].asDict()
    spark_row = spark_df.collect()[0].asDict()

    numeric_columns = set(_numeric_columns(flowpipe_df))
    for column, value in flowpipe_row.items():
        other_value = spark_row.get(column)
        if value is None and other_value is None:
            continue
        if column in numeric_columns:
            if value is None or other_value is None:
                print(f"Mismatch in column '{column}': {value} != {other_value}")
                return False
            if abs(value - other_value) > tolerance:
                print(
                    f"Mismatch in column '{column}': "
                    f"{value} != {other_value} (tolerance {tolerance})."
                )
                return False
        else:
            if value != other_value:
                print(f"Mismatch in column '{column}': {value} != {other_value}")
                return False

    return True


def _compare_with_keys(
    flowpipe_df,
    spark_df,
    key_columns: List[str],
    tolerance: float,
    sample_limit: int,
) -> bool:
    flowpipe_marked = flowpipe_df.withColumn("_flowpipe_present", lit(1))
    spark_marked = spark_df.withColumn("_spark_present", lit(1))

    flowpipe_prefixed = _prefixed_dataframe(flowpipe_marked, "flow_", key_columns)
    spark_prefixed = _prefixed_dataframe(spark_marked, "spark_", key_columns)

    joined = flowpipe_prefixed.join(spark_prefixed, on=key_columns, how="full")
    missing_rows = joined.filter(
        col("_flowpipe_present").isNull() | col("_spark_present").isNull()
    )
    missing_count = missing_rows.count()
    if missing_count:
        print(f"Found {missing_count} unmatched rows between Flow-Pipe and Spark.")
        missing_rows.show(sample_limit, truncate=False)
        return False

    numeric_columns = set(_numeric_columns(flowpipe_df))

    comparisons = []
    for column in flowpipe_df.columns:
        if column in key_columns:
            continue
        flow_column = col(f"flow_{column}")
        spark_column = col(f"spark_{column}")
        if column in numeric_columns:
            comparison = (
                (flow_column.isNull() & spark_column.isNull())
                | (abs_(flow_column - spark_column) <= tolerance)
            )
        else:
            comparison = (
                flow_column.isNull() & spark_column.isNull()
            ) | (flow_column == spark_column)
        comparisons.append(comparison)

    if not comparisons:
        return True

    matches_all = reduce(lambda left, right: left & right, comparisons)
    mismatches = joined.filter(~matches_all)
    mismatch_count = mismatches.count()
    if mismatch_count:
        print(
            "Detected mismatched rows between Flow-Pipe and Spark results: "
            f"{mismatch_count}."
        )
        mismatches.show(sample_limit, truncate=False)
        return False

    return True


def compare_results(
    pipeline: str,
    flowpipe_output: str,
    spark_output: str,
    tolerance: float,
    sample_limit: int,
) -> bool:
    config = PIPELINE_CONFIGS[pipeline]
    spark = SparkSession.builder.appName(f"validate-{pipeline}").getOrCreate()

    flowpipe_df = spark.read.parquet(flowpipe_output)
    spark_df = spark.read.parquet(spark_output)

    flowpipe_columns = set(flowpipe_df.columns)
    spark_columns = set(spark_df.columns)
    if flowpipe_columns != spark_columns:
        print("Column mismatch between Flow-Pipe and Spark outputs.")
        print(f"Flow-Pipe columns: {sorted(flowpipe_columns)}")
        print(f"Spark columns: {sorted(spark_columns)}")
        spark.stop()
        return False

    if config.key_columns:
        result = _compare_with_keys(
            flowpipe_df,
            spark_df,
            config.key_columns,
            tolerance,
            sample_limit,
        )
    else:
        result = _compare_single_row(flowpipe_df, spark_df, tolerance)

    spark.stop()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Flow-Pipe and Spark output parity"
    )
    parser.add_argument(
        "--pipeline",
        choices=sorted(PIPELINE_CONFIGS.keys()),
        required=True,
        help="Pipeline name to validate",
    )
    parser.add_argument("--flowpipe-output", help="Flow-Pipe output path")
    parser.add_argument("--spark-output", help="Spark output path")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-6,
        help="Numeric tolerance for comparisons",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=5,
        help="Rows to display when mismatches are found",
    )
    args = parser.parse_args()

    config = PIPELINE_CONFIGS[args.pipeline]
    flowpipe_output = args.flowpipe_output or config.flowpipe_output
    spark_output = args.spark_output or config.spark_output

    if not compare_results(
        args.pipeline,
        flowpipe_output,
        spark_output,
        args.tolerance,
        args.sample_limit,
    ):
        sys.exit(1)

    print("Validation successful: Flow-Pipe and Spark outputs match.")


if __name__ == "__main__":
    main()
