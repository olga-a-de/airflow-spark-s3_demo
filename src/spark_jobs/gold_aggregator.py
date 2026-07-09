import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CryptoSilverToGold")

def main():
    if len(sys.argv) < 2:
        logger.error("Missing business intelligence reporting processing date argument.")
        sys.exit(1)

    processing_date_str = sys.argv[1]
    
    try:
        processing_date = datetime.strptime(processing_date_str, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid business processing token format input: '{processing_date_str}'")
        sys.exit(1)
        
    year = f"{processing_date.year}"
    month = f"{processing_date.month:02d}"
    day = f"{processing_date.day:02d}"

    spark = SparkSession.builder \
        .appName("CryptoSilverToGold") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    try:
        input_path = f"s3a://silver/crypto_market/year={year}/month={month}/day={day}/"
        output_path = "s3a://gold/daily_crypto_metrics/"

        logger.info(f"Aggregating enterprise metrics matrices for processing dimension: {processing_date_str}")

        df_silver = spark.read.parquet(input_path)

        df_gold = df_silver.groupBy("asset").agg(
            F.round(F.avg("price"), 4).alias("avg_daily_price"),
            F.max("price").alias("max_daily_price"),
            F.min("price").alias("min_daily_price"),
            F.round(F.sum("volume"), 2).alias("total_daily_volume"),
            F.count("timestamp").alias("total_ticks_processed")
        )

        df_final = df_gold \
            .withColumn("year", F.lit(year)) \
            .withColumn("month", F.lit(month)) \
            .withColumn("day", F.lit(day))

        logger.info(f"Writing calculated analytical data frames out to Gold layer target: {output_path}")

        df_final.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)

        logger.info("Gold business metric cubes materialized successfully.")
    except Exception as e:
        logger.error(f"Data aggregation runtime execution process failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()