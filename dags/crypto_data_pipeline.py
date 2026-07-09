import sys
import logging
from datetime import datetime
from airflow import Asset
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

sys.path.append("/opt/airflow/dags/repo")
from src.extraction.api_extractor import extract_market_data

logger = logging.getLogger(__name__)

bronze_asset = Asset(uri="s3://bronze/crypto_market/", name="crypto_bronze_layer")
silver_asset = Asset(uri="s3://silver/crypto_market/", name="crypto_silver_layer")
gold_asset = Asset(uri="s3://gold/daily_crypto_metrics/", name="crypto_gold_layer")

@dag(
    dag_id="crypto_medallion_pipeline_v3",
    schedule="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crypto", "medallion_architecture", "airflow3_2_2_production"],
)
def crypto_data_platform_pipeline():

    @task(task_id="extract_raw_data")
    def run_extraction_task(logical_date=None) -> str:
        date_str = logical_date.strftime('%Y-%m-%d')
        return extract_market_data(date_str)

    local_file_path = run_extraction_task()

    upload_to_bronze = LocalFilesystemToS3Operator(
        task_id="upload_to_bronze",
        filename=local_file_path,
        dest_key="crypto_market/year={{ data_interval_start.strftime('%Y') }}/month={{ data_interval_start.strftime('%m') }}/day={{ data_interval_start.strftime('%d') }}/snapshot_{{ ts_nodash }}.json",
        dest_bucket="bronze",
        aws_conn_id="aws_default",
        replace=True,
        outlets=[bronze_asset]
    )

   # Task 3: Transform Bronze to Silver via Spark
    transform_bronze_to_silver = SparkSubmitOperator(
        task_id="transform_bronze_to_silver",
        conn_id="spark_default",
        application="/opt/airflow/dags/repo/src/spark_jobs/silver_transformer.py",
        application_args=["{{ data_interval_start.strftime('%Y-%m-%d') }}"],
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
       conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_default.login }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ conn.aws_default.password }}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl_enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3n.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.timeout": "5000",
            "spark.hadoop.fs.s3a.socket.timeout": "5000",
            "spark.hadoop.fs.s3a.connection.request.timeout": "5000",
            "spark.hadoop.fs.s3a.retry.limit": "2",
            "spark.hadoop.fs.s3a.retry.interval": "1s"
        },
        verbose=True,
        inlets=[bronze_asset],
        outlets=[silver_asset]
    )

    # Task 4: Aggregate Silver to Gold via Spark
    aggregate_silver_to_gold = SparkSubmitOperator(
        task_id="aggregate_silver_to_gold",
        conn_id="spark_default",
        application="/opt/airflow/dags/repo/src/spark_jobs/gold_aggregator.py",
        application_args=["{{ data_interval_start.strftime('%Y-%m-%d') }}"],
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
       conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_default.login }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ conn.aws_default.password }}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl_enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3n.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.timeout": "5000",
            "spark.hadoop.fs.s3a.socket.timeout": "5000",
            "spark.hadoop.fs.s3a.connection.request.timeout": "5000",
            "spark.hadoop.fs.s3a.retry.limit": "2",
            "spark.hadoop.fs.s3a.retry.interval": "1s"
        },
        verbose=True,
        inlets=[silver_asset],
        outlets=[gold_asset]
    )

    upload_to_bronze >> transform_bronze_to_silver >> aggregate_silver_to_gold

crypto_data_platform_pipeline()