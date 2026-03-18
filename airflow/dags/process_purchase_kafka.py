import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def process_purchase_streaming():
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("Kafka-to-Iceberg-Silver-Purchase-Strict-Deduplication") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://silver/") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # --- BƯỚC 1: KHỞI TẠO BẢNG SILVER ---
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_db")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver_db.amazon_purchase_silver (
            order_date DATE,
            unit_price DOUBLE,
            quantity DOUBLE,
            state STRING,
            product_title STRING,
            product_code STRING,
            category STRING,
            survey_id STRING,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(order_date))
    """)

    # 2. Định nghĩa Schema thô từ Kafka
    kafka_schema = StructType([
        StructField("Order Date", StringType(), True),
        StructField("Purchase Price Per Unit", DoubleType(), True),
        StructField("Quantity", DoubleType(), True),
        StructField("Shipping Address State", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("ASIN/ISBN (Product Code)", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Survey ResponseID", StringType(), True)
    ])

    # 3. Đọc dữ liệu từ Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "amazon_purchases") \
        .option("startingOffsets", "earliest") \
        .option("kafka.group.id", "reset_data_batch_cuoi_v99") \
        .load()

    # 4. Parse JSON và Chuẩn hóa
    df_clean = df_raw.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), kafka_schema).alias("data")) \
        .select("data.*") \
        .replace(['"NaN"', 'NaN', 'nan', '"nan"'], None) \
        .dropna() \
        .select(
            to_date(col("Order Date"), "yyyy-MM-dd").alias("order_date"),
            col("Purchase Price Per Unit").alias("unit_price"),
            col("Quantity").alias("quantity"),
            col("Shipping Address State").alias("state"),
            col("Title").alias("product_title"),
            col("ASIN/ISBN (Product Code)").alias("product_code"),
            col("Category").alias("category"),
            col("Survey ResponseID").alias("survey_id")
        ).withColumn("processed_at", current_timestamp())

    # 5. Hàm ghi Batch
    table_name = "nessie.silver_db.amazon_purchase_silver"
    checkpoint_path = "s3a://silver/checkpoints_silver/silver_purchase_stream_fixed_v1"

    def write_upsert_strict(batch_df, batch_id):
        total_count = batch_df.count()
        
        if total_count > 0:
            clean_batch = batch_df.distinct()
            distinct_count = clean_batch.count()
            
            # --- FIX NẰM Ở ĐÂY ---
            # Sử dụng chính Spark Session của batch_df để đăng ký View
            batch_df.sparkSession.catalog.dropTempView("tmp_batch") # Xóa view cũ cho chắc
            clean_batch.createOrReplaceTempView("tmp_batch")
            
            # Gọi MERGE thông qua Spark Session nội bộ của batch
            batch_df.sparkSession.sql(f"""
                MERGE INTO {table_name} t
                USING tmp_batch s
                ON t.survey_id = s.survey_id 
                   AND t.product_code = s.product_code 
                   AND t.order_date = s.order_date
                   AND t.unit_price = s.unit_price
                   AND t.quantity = s.quantity
                WHEN MATCHED THEN
                    UPDATE SET t.processed_at = s.processed_at
                WHEN NOT MATCHED THEN
                    INSERT *
            """)
            
            print(f"✅ BATCH {batch_id}: Đã nạp thành công {distinct_count} dòng sạch.")
        else:
            print(f"ℹ️ Batch {batch_id}: Trống.")

    # 6. Chạy luồng
    print("🚀 Đang bắt đầu nạp dữ liệu vào Iceberg...")
    query = df_clean.writeStream \
        .foreachBatch(write_upsert_strict) \
        .trigger(availableNow=True) \
        .option("checkpointLocation", checkpoint_path) \
        .start()

    query.awaitTermination()
    
    # In tổng kết
    final_total = spark.read.table(table_name).count()
    print(f"📊 Hoàn tất! Tổng số dòng trong kho Silver: {final_total}")

if __name__ == "__main__":
    process_purchase_streaming()