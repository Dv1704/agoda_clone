from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, sum as _sum
import sys

def main(input_path, output_path):
    spark = SparkSession.builder \
        .appName("AgodaDataTransformation") \
        .getOrCreate()

    # Read CSV data
    df = spark.read.option("header", "true").csv(input_path)

    # Simple transformation: Ensure types and add processing timestamp
    df_transformed = df.withColumn("amount", col("amount").cast("double")) \
                       .withColumn("timestamp", col("timestamp").cast("long")) \
                       .withColumn("processed_at", current_timestamp())

    # Write to Parquet (shadow testing destination)
    df_transformed.write.mode("overwrite").parquet(output_path)

    # Write to Postgres (The Persistence Layer)
    # Note: In a real local setup, ensure the jar is available
    try:
        df_transformed.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/agoda_data") \
            .option("dbtable", "financial_records") \
            .option("user", "agoda") \
            .option("password", "agoda") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print("✅ Successfully pushed to Postgres")
    except Exception as e:
        print(f"⚠️ Postgres push skipped (usually needs JDBC jar): {e}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: transform.py <input_path> <output_path>")
        sys.exit(1)
    
    main(sys.argv[1], sys.argv[2])
