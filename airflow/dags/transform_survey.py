import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, current_timestamp, lit
from datetime import datetime

def process_survey():
    # 1. Khởi tạo Spark Session với đầy đủ cấu hình Iceberg & Nessie
    spark = SparkSession.builder \
        .appName("Survey-Bronze-to-Iceberg-Silver-Incremental") \
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

    try:
        # --- BƯỚC 1: TẠO NAMESPACE ---
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver_db")

        # --- BƯỚC 2: ĐỌC DỮ LIỆU TỪ BUCKET BRONZE ---
        bronze_path = "s3a://bronze/survey/*.csv"
        
        try:
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("sep", ";") \
                .option("inferSchema", "true") \
                .load(bronze_path)
            
            # ÉP SPARK ĐỌC FILE NGAY LẬP TỨC VÀO BỘ NHỚ (Tránh lỗi FileNotFound sau này)
            df.cache()
            row_count = df.count()
            print(f"✅ Đã tìm thấy {row_count} dòng dữ liệu từ file CSV.")
            
        except Exception:
            print("⚠️ Không tìm thấy file mới hoặc thư mục survey trống. Thoát.")
            return

        # --- BƯỚC 3: TRANSFORM DỮ LIỆU ---
        df_silver = df.select(
            col("`Survey ResponseID`").alias("survey_id"),
            col("`Q-demos-age`").alias("age_group"),
            col("`Q-demos-race`").alias("race"),
            col("`Q-demos-hispanic`").alias("is_hispanic"),
            col("`Q-demos-education`").alias("education"),
            col("`Q-demos-income`").alias("income"),
            col("`Q-demos-gender`").alias("gender"),
            col("`Q-demos-state`").alias("state"),
            col("`Q-amazon-use-howmany`").alias("amazon_device_count_raw"),
            col("`Q-amazon-use-hh-size`").alias("household_size_raw"),
            col("`Q-amazon-use-how-oft`").alias("use_frequency")
        ).dropDuplicates(["survey_id"]).dropna()

        df_final = df_silver.withColumn(
            "household_size", regexp_extract(col("household_size_raw"), r"(\d+)", 1).cast("int")
        ).withColumn(
            "amazon_device_count", regexp_extract(col("amazon_device_count_raw"), r"(\d+)", 1).cast("int")
        ).drop("household_size_raw", "amazon_device_count_raw") \
         .dropna(subset=["household_size", "amazon_device_count"]) \
         .withColumn("processed_at", current_timestamp())

        # --- BƯỚC 4: GHI DỮ LIỆU DÙNG MERGE INTO (UPSERT) ---
        table_name = "nessie.silver_db.survey_silver"
        table_exists = spark.catalog.tableExists(table_name)
        
        if not table_exists:
            print(f"🏗️ Khởi tạo bảng {table_name} lần đầu...")
            df_final.writeTo(table_name).create()
        else:
            print(f"🔄 Đang thực hiện MERGE INTO {table_name}...")
            df_final.createOrReplaceTempView("source_view")
            
            # Nếu trùng ID thì cập nhật toàn bộ (UPDATE SET *), nếu không thì thêm mới
            spark.sql(f"""
                MERGE INTO {table_name} AS target
                USING source_view AS source
                ON target.survey_id = source.survey_id
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """)
        
        # Đảm bảo lệnh Write đã xong trước khi chạy tiếp
        print("💾 Dữ liệu đã được ghi vào Iceberg thành công.")

        # --- BƯỚC 5: DI CHUYỂN FILE SANG PROCESSING (CHỈ CHẠY KHI BƯỚC 4 XONG) ---
        print("🚚 Đang dọn dẹp file trong Bronze và chuyển sang Processing...")
        sc = spark.sparkContext
        jvm = sc._gateway.jvm
        
        # Cấu hình Hadoop FileSystem
        Path = jvm.org.apache.hadoop.fs.Path
        FileSystem = jvm.org.apache.hadoop.fs.FileSystem
        URI = jvm.java.net.URI
        conf = sc._jsc.hadoopConfiguration()
        
        # Fix lỗi Wrong FS bằng cách ép URI s3a://bronze
        fs = FileSystem.get(URI("s3a://bronze"), conf)

        src_dir = Path("s3a://bronze/survey/")
        dst_dir = Path("s3a://bronze/processing/")

        # Tạo thư mục processing nếu chưa có
        if not fs.exists(dst_dir):
            fs.mkdirs(dst_dir)

        # Liệt kê file và di chuyển
        files = fs.listStatus(src_dir)
        processed_files_count = 0
        
        if files:
            for f in files:
                file_path = f.getPath()
                if f.isFile() and file_path.getName().endswith(".csv"):
                    target_path = Path(dst_dir.toString() + "/" + file_path.getName())
                    fs.rename(file_path, target_path) # Đây là lệnh di chuyển
                    print(f"✔️ Đã di chuyển file thành công: {file_path.getName()}")
                    processed_files_count += 1

        print(f"✨ HOÀN TẤT: Đã xử lý {processed_files_count} file và cập nhật vào Silver.")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
        sys.exit(1)
    finally:
        # Giải phóng cache và dừng Spark
        if 'df' in locals(): df.unpersist()
        spark.stop()

if __name__ == "__main__":
    process_survey()