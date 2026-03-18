from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer
from minio import Minio  # Sử dụng thư viện Minio thay cho boto3
import os
import json
import shutil

# --- 1. CẤU HÌNH HỆ THỐNG ---
BASE_PATH = "/home/jovyan/notebooks/dataverse_files" 
LANDING_DIR = os.path.join(BASE_PATH, "landing")
PROCESSED_DIR = os.path.join(BASE_PATH, "processed")
STATE_FILE = os.path.join(BASE_PATH, "state_checkpoint.json")

# Kết nối
KAFKA_SERVER = 'kafka:29092' 
# Với thư viện Minio, bạn chỉ cần truyền hostname:port (không có http://)
MINIO_ADDR = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "bronze"

# --- 2. CÁC HÀM HỖ TRỢ ---

def get_last_checkpoint(file_name):
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f).get(file_name, 0)
        except: return 0
    return 0

def update_checkpoint(file_name, row_index):
    state = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
        except: state = {}
    state[file_name] = row_index
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

# --- 3. LOGIC XỬ LÝ ---

def process_survey():
    file_name = "survey.csv"
    input_path = os.path.join(LANDING_DIR, file_name)
    
    if not os.path.exists(input_path):
        print(f"--- [Survey] Không tìm thấy file {file_name}. Bỏ qua.")
        return

    print(f"--- [Survey] Bắt đầu khởi tạo kết nối MinIO Client...")
    
    # Khởi tạo Minio Client
    client = Minio(
        endpoint=MINIO_ADDR,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Kiểm tra và tạo bucket
    try:
        if not client.bucket_exists(BUCKET_NAME):
            print(f"--- [Survey] Đang tạo bucket mới: {BUCKET_NAME}")
            client.make_bucket(BUCKET_NAME)
        else:
            print(f"--- [Survey] Bucket '{BUCKET_NAME}' đã tồn tại.")
    except Exception as e:
        print(f"--- [Survey] Lỗi kiểm tra/tạo bucket: {e}")
        raise e

    # Định nghĩa tên file trên MinIO
    target_key = f"survey/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_name}"
    
    # Upload file bằng phương thức fput_object (chuyên cho file từ local disk)
    try:
        client.fput_object(BUCKET_NAME, target_key, input_path)
        print(f"--- [Survey] Đã upload thành công lên MinIO: {target_key}")
    except Exception as e:
        print(f"--- [Survey] Lỗi khi upload file: {e}")
        raise e
    
    # Di chuyển file sang processed
    if not os.path.exists(PROCESSED_DIR): 
        os.makedirs(PROCESSED_DIR)
    
    dest_path = os.path.join(PROCESSED_DIR, f"done_{datetime.now().timestamp()}_{file_name}")
    shutil.move(input_path, dest_path)
    print(f"--- [Survey] Hoàn tất di chuyển file.")

def process_purchase_to_kafka():
    file_name = "amazon-purchases.csv"
    input_path = os.path.join(LANDING_DIR, file_name)
    
    if not os.path.exists(input_path):
        print(f"--- [Purchase] Không tìm thấy file {file_name}. Bỏ qua.")
        return

    print(f"--- [Purchase] Đang kết nối Kafka tại {KAFKA_SERVER}...")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    df = pd.read_csv(input_path)
    last_row = get_last_checkpoint(file_name)
    total_rows = len(df)
    
    print(f"--- [Purchase] Bắt đầu gửi từ dòng {last_row}/{total_rows}")
    
    try:
        for i in range(last_row, total_rows):
            row_data = df.iloc[i].to_dict()
            producer.send('amazon_purchases', value=row_data)
            
            if i % 1000 == 0 and i > 0:
                update_checkpoint(file_name, i)
                print(f"--- [Purchase] Tiến trình: {i}/{total_rows} dòng...")
        
        producer.flush()
        update_checkpoint(file_name, 0)
        
        if not os.path.exists(PROCESSED_DIR): os.makedirs(PROCESSED_DIR)
        shutil.move(input_path, os.path.join(PROCESSED_DIR, f"done_{file_name}"))
        print(f"--- [Purchase] Hoàn tất gửi dữ liệu vào Kafka.")
        
    except Exception as e:
        print(f"--- [Purchase] LỖI: {e}")
        raise e

# --- 4. ĐỊNH NGHĨA DAG ---

with DAG(
    '1.Ingestion', 
    start_date=datetime(2026, 2, 23), 
    schedule_interval=None, 
    catchup=False,
    tags=['lakehouse', 'minio_native']
) as dag:

    task_survey = PythonOperator(
        task_id='survey_to_minio', 
        python_callable=process_survey
    )

    task_purchase = PythonOperator(
        task_id='purchase_to_kafka', 
        python_callable=process_purchase_to_kafka
    )

    task_survey
    task_purchase