import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as sum_
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def lineitem_schema():
    return StructType(
        [
            StructField("l_orderkey", LongType(), False),
            StructField("l_partkey", LongType(), False),
            StructField("l_suppkey", LongType(), False),
            StructField("l_linenumber", IntegerType(), False),
            StructField("l_quantity", DoubleType(), False),
            StructField("l_extendedprice", DoubleType(), False),
            StructField("l_discount", DoubleType(), False),
            StructField("l_tax", DoubleType(), False),
            StructField("l_returnflag", StringType(), False),
            StructField("l_linestatus", StringType(), False),
            StructField("l_shipdate", DateType(), False),
            StructField("l_commitdate", DateType(), False),
            StructField("l_receiptdate", DateType(), False),
            StructField("l_shipinstruct", StringType(), False),
            StructField("l_shipmode", StringType(), False),
            StructField("l_comment", StringType(), False),
        ]
    )


def read_lineitem(spark, path, fmt, delimiter):
    if fmt == "parquet":
        return spark.read.parquet(path)
    return (
        spark.read.format("csv")
        .option("delimiter", delimiter)
        .option("header", "false")
        .schema(lineitem_schema())
        .load(path)
    )


def main():
    parser = argparse.ArgumentParser(description="TPC-H Q6 (Spark)")
    parser.add_argument("--input", required=True, help="Path to lineitem data")
    parser.add_argument("--format", default="parquet", choices=["parquet", "csv"])
    parser.add_argument("--delimiter", default="|")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "csv"])
    args = parser.parse_args()

    spark = SparkSession.builder.appName("tpch-q6").getOrCreate()
    lineitem = read_lineitem(spark, args.input, args.format, args.delimiter)

    filtered = lineitem.filter(
        (col("l_shipdate") >= lit("1994-01-01").cast("date"))
        & (col("l_shipdate") < lit("1995-01-01").cast("date"))
        & (col("l_discount") >= 0.05)
        & (col("l_discount") <= 0.07)
        & (col("l_quantity") < 24)
    )

    result = filtered.select(sum_(col("l_extendedprice") * col("l_discount")).alias("revenue"))

    if args.output_format == "parquet":
        result.write.mode("overwrite").parquet(args.output)
    else:
        result.write.mode("overwrite").option("header", True).csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
