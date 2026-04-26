import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def purchase_silver_stream():
    # 1. Spark Session cho lớp SILVER
    spark = SparkSession.builder \
        .appName("Bronze-to-Silver-Purchase") \
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

    # 2. Schema parse JSON
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

    # 3. Tạo bảng Silver (Iceberg)
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

    # 4. Đọc Bronze stream
    df_bronze = spark.readStream \
        .format("iceberg") \
        .load("nessie.bronze_db.amazon_purchase_raw")

    # 5. Parse JSON + Làm sạch dữ liệu chuyên sâu
    # Bước này tích hợp các yêu cầu xử lý NaN và Null của bạn
    df_parsed = df_bronze.select(
        F.from_json(F.col("kafka_value"), kafka_schema).alias("data")
    ).select("data.*")

    # A & B: Xử lý ép kiểu ngày và xử lý các chuỗi "NaN" về Null
    # Trong Streaming, dùng F.when() thay cho .replace() để đạt hiệu suất tốt hơn
    bad_values = ['"NaN"', 'NaN', 'nan', '"nan"']
    
    df_transformed = df_parsed
    for column in df_parsed.columns:
        df_transformed = df_transformed.withColumn(
            column, 
            F.when(F.col(column).cast("string").isin(bad_values), F.lit(None)).otherwise(F.col(column))
        )

    # C: Ép kiểu ngày và Loại bỏ null (dropna)
    df_clean = df_transformed \
        .withColumn("Order Date", F.to_date(F.col("Order Date"), "yyyy-MM-dd")) \
        .dropna() \
        .select(
            F.col("Order Date").alias("order_date"),
            F.col("Purchase Price Per Unit").alias("unit_price"),
            F.col("Quantity").alias("quantity"),
            F.col("Shipping Address State").alias("state"),
            F.col("Title").alias("product_title"),
            F.col("ASIN/ISBN (Product Code)").alias("product_code"),
            F.col("Category").alias("category"),
            F.col("Survey ResponseID").alias("survey_id")
        ).withColumn("processed_at", F.current_timestamp())

    # 🔥 Biến điều khiển dừng stream
    empty_batch_count = {"count": 0}
    MAX_EMPTY = 2

    def merge_silver(batch_df, batch_id):
        # D. Kiểm tra nhanh kết quả trong mỗi batch (thay cho print count ở ngoài)
        row_count = batch_df.count()
        
        if row_count == 0:
            empty_batch_count["count"] += 1
            print(f"⚠ Batch {batch_id} rỗng ({empty_batch_count['count']})")
            if empty_batch_count["count"] >= MAX_EMPTY:
                print("✅ DONE - Không còn dữ liệu, dừng stream!")
                query.stop()
            return
        else:
            empty_batch_count["count"] = 0
            print(f"🚀 Batch {batch_id} xử lý xong! Số dòng hợp lệ: {row_count}")

        batch_df.createOrReplaceTempView("tmp_silver")

        batch_df.sparkSession.sql("""
            MERGE INTO nessie.silver_db.amazon_purchase_silver t
            USING tmp_silver s
            ON  t.survey_id = s.survey_id
            AND t.product_code = s.product_code
            AND t.order_date = s.order_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✔ Batch {batch_id} đã được merged vào Silver")

    # 🚀 Start stream
    query = df_clean.writeStream \
        .foreachBatch(merge_silver) \
        .option("checkpointLocation", "s3a://silver/checkpoints/silver_purchase_v2") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()
    
    # In tổng kết cuối cùng
    final_total = spark.read.table("nessie.silver_db.amazon_purchase_silver").count()
    print(f"📊 Hoàn tất! Tổng số dòng trong kho Silver: {final_total}")

if __name__ == "__main__":
    purchase_silver_stream()