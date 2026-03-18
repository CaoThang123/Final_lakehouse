import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from itertools import chain

# 1. KHỞI TẠO SPARK SESSION
def get_spark_session():
    return SparkSession.builder \
        .appName("Gold-Layer-Standard-v5") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://gold/") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

# 2. CÁC HÀM XÂY DỰNG CHIỀU (DIMENSIONS)

def build_dim_customer(df_survey_silver):
    """Khớp chính xác các cột Thăng cung cấp từ Survey Silver"""
    return df_survey_silver.select(
        col("survey_id").alias("customer_id"),
        "age_group", "race", "is_hispanic", "education", 
        "income", "gender", "state", "use_frequency", 
        "household_size", "amazon_device_count"
    ).dropDuplicates(["customer_id"])

def build_dim_product(df_purchase_batch):
    return df_purchase_batch.select(
        col("product_code").alias("product_id"),
        col("product_title"),
        col("category").alias("product_category")
    ).dropDuplicates(["product_id"])

def build_dim_time(df_purchase_batch):
    dim_time = df_purchase_batch.select(col("order_date")).dropDuplicates()
    return dim_time.withColumn("year", year("order_date")) \
                   .withColumn("month", month("order_date")) \
                   .withColumn("day", dayofmonth("order_date")) \
                   .withColumn("quarter", quarter("order_date")) \
                   .withColumn("weekday_name", date_format("order_date", "EEEE")) \
                   .withColumn("time_id", date_format("order_date", "yyyyMMdd").cast("long"))

def build_dim_location(df_purchase_batch):
    """Mapping đầy đủ 50 tiểu bang và Vùng miền (Region) Hoa Kỳ"""
    states_data = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
        "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
        "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
        "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
        "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
        "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
        "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
        "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia"
    }
    
    regions_data = {
        "WA": "West", "OR": "West", "CA": "West", "NV": "West", "ID": "West", "MT": "West", "WY": "West", "UT": "West", "AZ": "West", "CO": "West", "NM": "West", "AK": "West", "HI": "West",
        "ND": "Midwest", "SD": "Midwest", "NE": "Midwest", "KS": "Midwest", "MN": "Midwest", "IA": "Midwest", "MO": "Midwest", "WI": "Midwest", "IL": "Midwest", "MI": "Midwest", "IN": "Midwest", "OH": "Midwest",
        "TX": "South", "OK": "South", "AR": "South", "LA": "South", "MS": "South", "AL": "South", "TN": "South", "KY": "South", "GA": "South", "FL": "South", "SC": "South", "NC": "South", "VA": "South", "WV": "South", "MD": "South", "DE": "South", "DC": "South",
        "ME": "Northeast", "NH": "Northeast", "VT": "Northeast", "MA": "Northeast", "RI": "Northeast", "CT": "Northeast", "NY": "Northeast", "NJ": "Northeast", "PA": "Northeast"
    }

    name_map = create_map([lit(x) for x in chain(*states_data.items())])
    region_map = create_map([lit(x) for x in chain(*regions_data.items())])

    return df_purchase_batch.select(col("state").alias("state_code")).dropDuplicates() \
                    .withColumn("state_name", coalesce(name_map[col("state_code")], col("state_code"))) \
                    .withColumn("region", coalesce(region_map[col("state_code")], lit("Other"))) \
                    .withColumn("location_id", monotonically_increasing_id())

# 3. HÀM UPSERT DIMENSION

def upsert_dim(df, table_name, join_key):
    """Đăng ký view nội bộ để thực hiện MERGE INTO"""
    df.createOrReplaceTempView("batch_updates")
    df.sparkSession.sql(f"""
        MERGE INTO nessie.gold_db.{table_name} t
        USING batch_updates s ON t.{join_key} = s.{join_key}
        WHEN NOT MATCHED THEN INSERT *
    """)

# 4. HÀM XỬ LÝ MICRO-BATCH

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0: return
    
    spark = batch_df.sparkSession
    df_s_silver = spark.table("nessie.silver_db.survey_silver")

    # Xây dựng Dimensions
    d_cust = build_dim_customer(df_s_silver)
    d_prod = build_dim_product(batch_df)
    d_time = build_dim_time(batch_df)
    d_loc  = build_dim_location(batch_df)
    
    # Thực hiện nạp Dimension (Fix lỗi thiếu tham số ở đây)
    upsert_dim(d_cust, "dim_customer", "customer_id")
    upsert_dim(d_prod, "dim_product", "product_id")
    upsert_dim(d_time, "dim_time", "order_date") 
    upsert_dim(d_loc, "dim_location", "state_code")
    
    # Xây dựng và nạp Fact Order
    f_order = batch_df \
        .join(d_time, "order_date", "left") \
        .join(d_prod, batch_df["product_code"] == d_prod["product_id"], "left") \
        .join(d_cust, batch_df["survey_id"] == d_cust["customer_id"], "left") \
        .join(d_loc, batch_df["state"] == d_loc["state_code"], "left") \
        .select(
            "time_id", "customer_id", "product_id", "location_id",
            col("unit_price"), col("quantity"),
            (col("unit_price") * col("quantity")).alias("total_revenue")
        )

    f_order.writeTo("nessie.gold_db.fact_order").append()
    print(f"✅ Batch {batch_id}: Đồng bộ Gold Layer (Star Schema) thành công.")

# 5. CHƯƠNG TRÌNH CHÍNH

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold_db")
    
    # Kiểm tra và khởi tạo bảng Gold
    table_list = [t.name for t in spark.catalog.listTables("nessie.gold_db")]
    
    if "fact_order" not in table_list:
        print("🏗️ Đang khởi tạo các bảng Gold lần đầu...")
        df_a_0 = spark.table("nessie.silver_db.amazon_purchase_silver").limit(0)
        df_s_0 = spark.table("nessie.silver_db.survey_silver").limit(0)
        
        build_dim_customer(df_s_0).writeTo("nessie.gold_db.dim_customer").create()
        build_dim_product(df_a_0).writeTo("nessie.gold_db.dim_product").create()
        build_dim_time(df_a_0).writeTo("nessie.gold_db.dim_time").create()
        build_dim_location(df_a_0).writeTo("nessie.gold_db.dim_location").create()
        
        # Khởi tạo Fact rỗng để lấy Schema
        d_t = build_dim_time(df_a_0)
        d_p = build_dim_product(df_a_0)
        d_c = build_dim_customer(df_s_0)
        d_l = build_dim_location(df_a_0)
        
        f_o_0 = df_a_0.join(d_t, "order_date", "left") \
                      .join(d_p, df_a_0["product_code"] == d_p["product_id"], "left") \
                      .join(d_c, df_a_0["survey_id"] == d_c["customer_id"], "left") \
                      .join(d_l, df_a_0["state"] == d_l["state_code"], "left") \
                      .select("time_id", "customer_id", "product_id", "location_id",
                              col("unit_price"), col("quantity"), 
                              lit(0.0).alias("total_revenue"))
        f_o_0.writeTo("nessie.gold_db.fact_order").create()

    # STREAMING INCREMENTAL
    df_amazon_stream = spark.readStream.format("iceberg").load("nessie.silver_db.amazon_purchase_silver")

    query = df_amazon_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "s3a://gold/checkpoints/gold_reset_v10") \
        .trigger(availableNow=True) \
        .start()

    query.awaitTermination()
    
    total = spark.read.table("nessie.gold_db.fact_order").count()
    print(f"🎉 HOÀN TẤT! Đã nạp xong {total} dòng vào Fact Order.")