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

    # 1. Tạo Database và Bảng nếu chưa có (KHÔNG DROP)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze_db")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze_db.amazon_purchase_raw (
            kafka_value STRING,
            kafka_timestamp TIMESTAMP
        ) USING iceberg
    """)

    # 2. Đọc dữ liệu từ Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "amazon_purchases") \
        .option("startingOffsets", "earliest") \
        .option("kafka.group.id", "bronze_consumer_group_v1") \
        .load()

    df_bronze = df_raw.selectExpr(
        "CAST(value AS STRING) AS kafka_value",
        "timestamp AS kafka_timestamp"
    )

    empty_batch_count = {"count": 0}
    MAX_EMPTY = 3  

    def process_batch(batch_df, batch_id):
        row_count = batch_df.count()
        
        if row_count == 0:
            empty_batch_count["count"] += 1
            if empty_batch_count["count"] >= MAX_EMPTY:
                print("✅ Hết dữ liệu mới, dừng stream.")
                query.stop()  
            return
        else:
            empty_batch_count["count"] = 0
            
            # 3. Dùng MERGE INTO để tránh trùng lặp dữ liệu (Idempotent)
            # Chúng ta check dựa trên nội dung và thời gian của kafka
            batch_df.createOrReplaceTempView("batch_view")
            
            batch_df.sparkSession.sql("""
                MERGE INTO nessie.bronze_db.amazon_purchase_raw t
                USING batch_view s
                ON t.kafka_value = s.kafka_value 
                   AND t.kafka_timestamp = s.kafka_timestamp
                WHEN NOT MATCHED THEN 
                   INSERT *
            """)
            print(f"✔ Batch {batch_id} (có {row_count} dòng) đã được MERGE vào Bronze.")

    # 4. Start Stream với availableNow
    query = df_bronze.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "s3a://bronze/checkpoints/bronze_purchase_final") \
        .trigger(availableNow=True) \
        .start()

    try:
        query.awaitTermination()
        final_total = spark.read.table("nessie.bronze_db.amazon_purchase_raw").count()
        print(f"📊 TỔNG KẾT: Tầng Bronze hiện có {final_total} dòng.")
    except Exception as e:
        print(f"Stream stopped: {e}")

if __name__ == "__main__":
    purchase_bronze_stream()