import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CryptoBronzeToSilver")

def main():
    if len(sys.argv) < 2:
        logger.error("Missing mandatory system execution business date argument.")
        sys.exit(1)

    processing_date_str = sys.argv[1]
    
    try:
        processing_date = datetime.strptime(processing_date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Malformed runtime execution date parameter format: '{processing_date_str}'. Use YYYY-MM-DD.")
        sys.exit(1)
        
    year = f"{processing_date.year}"
    month = f"{processing_date.month:02d}"
    day = f"{processing_date.day:02d}"

    minio_user = os.environ.get("MINIO_USER", "minioadmin")
    minio_pass = os.environ.get("MINIO_PASSWORD", "minioadmin")

    spark = (SparkSession.builder
        .appName("CryptoBronzeToSilver")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.extraJavaOptions", "-Djava.security.egd=file:/dev/./urandom")
        .getOrCreate())

    try:
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("asset_ticker", StringType(), True),
            StructField("price_open", DoubleType(), True),
            StructField("price_high", DoubleType(), True),
            StructField("price_low", DoubleType(), True),
            StructField("price_close", DoubleType(), True),
            StructField("volume_24h", DoubleType(), True),
            StructField("engine_metadata_load_id", LongType(), True)
        ])

        input_path = f"s3a://bronze/crypto_market/year={year}/month={month}/day={day}/*.json"
        output_path = "s3a://silver/crypto_market/"

        logger.info(f"Beginning Silver layer partitioning execution for date: {processing_date_str}")
        
        df_raw = spark.read.schema(schema).json(input_path)
        df_deduplicated = df_raw.dropDuplicates(["asset_ticker", "timestamp"])

        df_final = df_deduplicated \
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumnRenamed("asset_ticker", "asset") \
            .withColumnRenamed("price_close", "price") \
            .withColumnRenamed("volume_24h", "volume") \
            .withColumn("year", lit(year)) \
            .withColumn("month", lit(month)) \
            .withColumn("day", lit(day))

        logger.info(f"Persisting clean data structures to Parquet storage engine at: {output_path}")

        df_final.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)

        logger.info("Silver layer state synchronization completed successfully.")
    except Exception as e:
        logger.error(f"Critical unhandled exception encountered during Spark workflow execution: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()