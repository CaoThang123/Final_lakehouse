import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
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

    # 3. Tạo bảng Silver (nếu chưa có)
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
    # Lưu ý: Thắng nên dùng tên bảng mới nhất (raw_v2 hoặc raw tùy theo bước dọn dẹp trước đó)
    df_bronze = spark.readStream \
        .format("iceberg") \
        .load("nessie.bronze_db.amazon_purchase_raw")

    # 5. Parse JSON + làm sạch dữ liệu
    df_clean = df_bronze.select(
        from_json(col("kafka_value"), kafka_schema).alias("data")
    ).select("data.*") \
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

    # 🔥 Logic Batch
    empty_batch_count = {"count": 0}
    MAX_EMPTY = 2 

    def merge_silver(batch_df, batch_id):
        # SỬA: Dùng count() thay vì rdd.isEmpty() để tránh Python Version Mismatch
        row_count = batch_df.count()
        
        if row_count == 0:
            empty_batch_count["count"] += 1
            print(f"⚠ Batch {batch_id} rỗng ({empty_batch_count['count']}/{MAX_EMPTY})")
            if empty_batch_count["count"] >= MAX_EMPTY:
                print("✅ DONE - Hết dữ liệu cũ, dừng stream!")
                query.stop()
            return
        else:
            empty_batch_count["count"] = 0

        # Tạo view tạm để MERGE
        batch_df.createOrReplaceTempView("tmp_silver")

        # Thực hiện MERGE INTO
        batch_df.sparkSession.sql("""
            MERGE INTO nessie.silver_db.amazon_purchase_silver t
            USING tmp_silver s
            ON  t.survey_id = s.survey_id
            AND t.product_code = s.product_code
            AND t.order_date = s.order_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✔ Batch {batch_id} (có {row_count} dòng) merged vào Silver thành công.")

    # 🚀 Khởi chạy Stream với Trigger AvailableNow (Phù hợp nhất cho Airflow)
    query = df_clean.writeStream \
        .foreachBatch(merge_silver) \
        .option("checkpointLocation", "s3a://silver/checkpoints/silver_purchase_v1_final") \
        .trigger(availableNow=True) \
        .start()

    # Đợi Job hoàn tất
    try:
        query.awaitTermination()
        
        # In tổng kết (Chạy sau khi query dừng)
        final_total = spark.read.table("nessie.silver_db.amazon_purchase_silver").count()
        print("-" * 50)
        print(f"📊 TỔNG KẾT: Kho Silver hiện tại có {final_total} dòng.")
        print("-" * 50)
    except Exception as e:
        print(f"Stream kết thúc: {e}")

if __name__ == "__main__":
    purchase_silver_stream()