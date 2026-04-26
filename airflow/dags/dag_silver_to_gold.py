from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from datetime import timedelta

with DAG(
    dag_id='3.dag_silver_to_gold',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Để None để bạn chủ động Trigger tay hoặc gọi sau DAG Kafka
    catchup=False,
    tags=['iceberg', 'gold', 'nessie'],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30)  # 🔥 FIX CHÍNH
    }
) as dag :

    sync_gold_layer = SparkSubmitOperator(
        task_id='execute_process_gold_layer',
        conn_id='spark_conn',
        # Đường dẫn tới file script xử lý Gold
        application='/opt/airflow/dags/process_gold_layer.py', 
        conf={
            # Sử dụng đúng các packages bạn đang dùng cho Iceberg và Nessie
            "spark.jars.packages": (
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.7,"
                "org.apache.hadoop:hadoop-aws:3.3.4"
            ),
            # Đảm bảo Spark Master trỏ đúng cluster của bạn
            "spark.master": "spark://spark-master:7077",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g"
        }
    )

    sync_gold_layer