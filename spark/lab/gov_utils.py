# spark/lab/gov_utils.py
import os
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

def new_run_id() -> str:
    return str(uuid.uuid4())

def get_git_commit() -> str:
    return os.getenv("GIT_COMMIT", "unknown")

def log_job_run(
    spark: SparkSession,
    run_id: str,
    job_name: str,
    source_path: str,
    target_path: str,
    input_rows: int,
    output_rows: int,
    dq_failed_rows: int = 0,
    status: str = "SUCCESS",
    error_message: str = ""
):
    gov_path = "data/governance/job_runs"
    df = spark.createDataFrame([{
        "run_id": run_id,
        "job_name": job_name,
        "git_commit": get_git_commit(),
        "source_path": source_path,
        "target_path": target_path,
        "input_rows": int(input_rows),
        "output_rows": int(output_rows),
        "dq_failed_rows": int(dq_failed_rows),
        "status": status,
        "error_message": error_message,
        "logged_at": datetime.utcnow().isoformat()
    }])
    df.write.mode("append").parquet(gov_path)

def snapshot_schema(spark: SparkSession, df, dataset_name: str, dataset_path: str):
    """
    Lưu schema registry tối giản: dataset, path, schema_json, time
    """
    reg_path = "data/governance/schema_registry"
    schema_json = df.schema.json()
    snap = spark.createDataFrame([{
        "dataset_name": dataset_name,
        "dataset_path": dataset_path,
        "schema_json": schema_json,
        "snap_at": datetime.utcnow().isoformat()
    }])
    snap.write.mode("append").parquet(reg_path)