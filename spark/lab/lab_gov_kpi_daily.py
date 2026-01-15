# spark/lab/lab_gov_kpi_daily.py
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, broadcast

from gov_utils import new_run_id, log_job_run, snapshot_schema

SILVER_FACT = "data/silver/orders_fact_dt_stream"
CUST_DIM = "data/silver_lab32/customers_dim"
MER_DIM = "data/silver_lab32/merchants_dim"
GOLD_OUT = "data/gold/kpi_daily"

# keep schema snapshots separate from the data output path to avoid mixing files
SCHEMA_SNAP_OUT = "data/gov/schema_registry/gold_kpi_daily"


spark = (
    SparkSession.builder
    .appName("lab_gov_kpi_daily")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

run_id = new_run_id()
job_name = "gold_kpi_daily"

try:
    fact = (
        spark.read.parquet(SILVER_FACT)
        .where(col("dt") == "2026-01-10")
        .select("dt", "country", "customer_id", "merchant_id", "amount")
        .alias("f")
    )

    cust = (
        spark.read.parquet(CUST_DIM)
        .select("customer_id", "segment", "risk_tier")
        .alias("c")
    )

    mer = (
        spark.read.parquet(MER_DIM)
        .select("merchant_id", "mcc", "merchant_tier")
        .alias("m")
    )

    input_rows = fact.count()

    j = (
        fact
        .join(broadcast(cust), "customer_id", "left")
        .join(broadcast(mer), "merchant_id", "left")
    )

    kpi = (
        j.groupBy("dt", "country", "segment", "risk_tier", "mcc", "merchant_tier")
        .agg(
            count("*").alias("txns"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
        )
    )

    # schema registry snapshot
    snapshot_schema(spark, kpi, "gold.kpi_daily", SCHEMA_SNAP_OUT)

    # write gold
    # NOTE: In local labs it's common to rerun with different write layouts.
    # If an old run wrote `dt` as a normal column AND later runs partition by `dt`,
    # Spark can infer `dt` twice (from files + from partition folders) and throw
    # [COLUMN_ALREADY_EXISTS]. The simplest, most reliable fix for the lab is to
    # fully clean the target path before writing.
    # Clean output to avoid [COLUMN_ALREADY_EXISTS] when switching layouts across reruns
    if os.path.exists(GOLD_OUT):
        shutil.rmtree(GOLD_OUT)
    os.makedirs(os.path.dirname(GOLD_OUT), exist_ok=True)

    (kpi
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(GOLD_OUT)
    )

    # count AFTER write by reading the written dataset (avoids any cached plan issues)
    output_rows = spark.read.parquet(GOLD_OUT).count()

    log_job_run(
        spark=spark,
        run_id=run_id,
        job_name=job_name,
        source_path=SILVER_FACT,
        target_path=GOLD_OUT,
        input_rows=input_rows,
        output_rows=output_rows,
        dq_failed_rows=0,
        status="SUCCESS",
        error_message=""
    )

    print(f"✅ {job_name} done. run_id={run_id}")

except Exception as e:
    log_job_run(
        spark=spark,
        run_id=run_id,
        job_name=job_name,
        source_path=SILVER_FACT,
        target_path=GOLD_OUT,
        input_rows=0,
        output_rows=0,
        dq_failed_rows=0,
        status="FAILED",
        error_message=str(e)[:1000]
    )
    raise

finally:
    spark.stop()