import sys
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryException

def purchase_bronze_stream():
    spark = SparkSession.builder \
        .appName("Kafka-to-Iceberg-Bronze-Purchase") \
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://bronze/") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    
    # Create DB + table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze_db")

    spark.sql("DROP TABLE IF EXISTS nessie.bronze_db.amazon_purchase_raw")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze_db.amazon_purchase_raw (
            kafka_value STRING,
            kafka_timestamp TIMESTAMP
        ) USING iceberg
    """)

    # Read from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "amazon_purchases") \
        .option("startingOffsets", "earliest") \
        .option("kafka.group.id", "reset_data_batch_cuoi_v99") \
        .load()

    df_bronze = df_raw.selectExpr(
        "CAST(value AS STRING) AS kafka_value",
        "timestamp AS kafka_timestamp"
    )

    # 🔥 Biến đếm batch rỗng
    empty_batch_count = {"count": 0}
    MAX_EMPTY = 3  # sau 3 batch rỗng thì dừng

    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            empty_batch_count["count"] += 1
            print(f"⚠ Batch {batch_id} rỗng ({empty_batch_count['count']})")

            if empty_batch_count["count"] >= MAX_EMPTY:
                print("✅ DONE - Không còn dữ liệu, dừng stream!")
                query.stop()   # 👈 stop stream
        else:
            empty_batch_count["count"] = 0
            batch_df.write \
                .format("iceberg") \
                .mode("append") \
                .save("nessie.bronze_db.amazon_purchase_raw")

    query = df_bronze.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "s3a://bronze/checkpoints/bronze_purchase") \
        .start()

    try:
        query.awaitTermination()
    except StreamingQueryException:
        print("Stream stopped.")

if __name__ == "__main__":
    purchase_bronze_stream()