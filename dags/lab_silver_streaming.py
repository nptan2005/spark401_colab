from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="silver_orders_streaming",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    silver_orders_streaming = SparkSubmitOperator(
        task_id="silver_orders_streaming",
        conn_id="spark_local",
        application="/opt/spark/jobs/silver_streaming_from_kafka.py",
        name="silver_streaming_orders",
        verbose=True,
        deploy_mode="client",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1",
        jars="/opt/jars/openlineage-spark_2.13-1.40.1.jar",
        conf={
            "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
            "spark.openlineage.transport.type": "http",
            "spark.openlineage.transport.url": "http://marquez-api:5000/api/v1/lineage",
            "spark.openlineage.namespace": "spark401",

            # MinIO
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",

            # Java 17 module opens
            "spark.driver.extraJavaOptions": "--add-opens=java.base/java.security=ALL-UNNAMED "
                                            "--add-opens=java.base/java.lang=ALL-UNNAMED "
                                            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                                            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "spark.executor.extraJavaOptions": "--add-opens=java.base/java.security=ALL-UNNAMED "
                                            "--add-opens=java.base/java.lang=ALL-UNNAMED "
                                            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                                            "--add-opens=java.base/java.util=ALL-UNNAMED",
        },
        env_vars={
            "MINIO_ENDPOINT": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin123",

            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_TOPIC": "orders_raw",   # hoặc "orders"
            "OUTPUT_PATH": "s3a://lakehouse/silver/orders",
            "CHECKPOINT_PATH": "s3a://lakehouse/checkpoints/silver/orders",
        },
    )