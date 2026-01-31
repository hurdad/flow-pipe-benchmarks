import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, sum as sum_
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


def part_schema():
    return StructType(
        [
            StructField("p_partkey", LongType(), False),
            StructField("p_name", StringType(), False),
            StructField("p_mfgr", StringType(), False),
            StructField("p_brand", StringType(), False),
            StructField("p_type", StringType(), False),
            StructField("p_size", IntegerType(), False),
            StructField("p_container", StringType(), False),
            StructField("p_retailprice", DoubleType(), False),
            StructField("p_comment", StringType(), False),
        ]
    )


def read_table(spark, path, schema, fmt, delimiter):
    if fmt == "parquet":
        return spark.read.parquet(path)
    return (
        spark.read.format("csv")
        .option("delimiter", delimiter)
        .option("header", "false")
        .schema(schema)
        .load(path)
    )


def main():
    parser = argparse.ArgumentParser(description="TPC-H Q14 (Spark)")
    parser.add_argument("--lineitem", required=True, help="Path to lineitem data")
    parser.add_argument("--part", required=True, help="Path to part data")
    parser.add_argument("--format", default="parquet", choices=["parquet", "csv"])
    parser.add_argument("--delimiter", default="|")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "csv"])
    args = parser.parse_args()

    spark = SparkSession.builder.appName("tpch-q14").getOrCreate()
    lineitem = read_table(spark, args.lineitem, lineitem_schema(), args.format, args.delimiter)
    part = read_table(spark, args.part, part_schema(), args.format, args.delimiter)

    filtered = lineitem.filter(
        (col("l_shipdate") >= lit("1995-09-01").cast("date"))
        & (col("l_shipdate") < lit("1995-10-01").cast("date"))
    )

    joined = filtered.join(part, filtered.l_partkey == part.p_partkey, "inner")
    promo_revenue = (
        joined.select(
            sum_(
                expr(
                    "CASE WHEN p_type LIKE 'PROMO%' "
                    "THEN l_extendedprice * (1 - l_discount) ELSE 0 END"
                )
            ).alias("promo_sum"),
            sum_(col("l_extendedprice") * (1 - col("l_discount"))).alias("total_sum"),
        )
        .select((col("promo_sum") * 100.0 / col("total_sum")).alias("promo_revenue"))
    )

    if args.output_format == "parquet":
        promo_revenue.write.mode("overwrite").parquet(args.output)
    else:
        promo_revenue.write.mode("overwrite").option("header", True).csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
