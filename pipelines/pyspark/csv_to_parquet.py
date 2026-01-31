import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def build_schemas():
    return {
        "lineitem": StructType(
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
        ),
        "part": StructType(
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
        ),
        "orders": StructType(
            [
                StructField("o_orderkey", LongType(), False),
                StructField("o_custkey", LongType(), False),
                StructField("o_orderstatus", StringType(), False),
                StructField("o_totalprice", DoubleType(), False),
                StructField("o_orderdate", DateType(), False),
                StructField("o_orderpriority", StringType(), False),
                StructField("o_clerk", StringType(), False),
                StructField("o_shippriority", IntegerType(), False),
                StructField("o_comment", StringType(), False),
            ]
        ),
        "customer": StructType(
            [
                StructField("c_custkey", LongType(), False),
                StructField("c_name", StringType(), False),
                StructField("c_address", StringType(), False),
                StructField("c_nationkey", LongType(), False),
                StructField("c_phone", StringType(), False),
                StructField("c_acctbal", DoubleType(), False),
                StructField("c_mktsegment", StringType(), False),
                StructField("c_comment", StringType(), False),
            ]
        ),
        "supplier": StructType(
            [
                StructField("s_suppkey", LongType(), False),
                StructField("s_name", StringType(), False),
                StructField("s_address", StringType(), False),
                StructField("s_nationkey", LongType(), False),
                StructField("s_phone", StringType(), False),
                StructField("s_acctbal", DoubleType(), False),
                StructField("s_comment", StringType(), False),
            ]
        ),
        "partsupp": StructType(
            [
                StructField("ps_partkey", LongType(), False),
                StructField("ps_suppkey", LongType(), False),
                StructField("ps_availqty", IntegerType(), False),
                StructField("ps_supplycost", DoubleType(), False),
                StructField("ps_comment", StringType(), False),
            ]
        ),
        "nation": StructType(
            [
                StructField("n_nationkey", LongType(), False),
                StructField("n_name", StringType(), False),
                StructField("n_regionkey", LongType(), False),
                StructField("n_comment", StringType(), False),
            ]
        ),
        "region": StructType(
            [
                StructField("r_regionkey", LongType(), False),
                StructField("r_name", StringType(), False),
                StructField("r_comment", StringType(), False),
            ]
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Convert TPCH CSV files to Parquet")
    parser.add_argument("--input-base", required=True, help="Base path for TPCH CSV files")
    parser.add_argument("--output-base", required=True, help="Base path for Parquet output")
    parser.add_argument("--input-ext", default="csv", help="Input file extension")
    parser.add_argument("--delimiter", default="|", help="CSV delimiter")
    parser.add_argument(
        "--tables",
        default="lineitem,orders,part,partsupp,customer,supplier,nation,region",
        help="Comma-separated list of TPCH tables to convert",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("tpch-csv-to-parquet").getOrCreate()
    schemas = build_schemas()

    for table in args.tables.split(","):
        table = table.strip()
        if not table:
            continue
        schema = schemas.get(table)
        if schema is None:
            raise ValueError(f"Unknown table: {table}")
        input_path = f"{args.input_base}/{table}.{args.input_ext}"
        output_path = f"{args.output_base}/{table}"
        df = (
            spark.read.format("csv")
            .option("delimiter", args.delimiter)
            .option("header", "false")
            .option("mode", "DROPMALFORMED")
            .schema(schema)
            .load(input_path)
        )
        df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
