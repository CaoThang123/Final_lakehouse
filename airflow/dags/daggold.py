from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30)
}

with DAG(
    dag_id='3.dag_silver_to_gold',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['iceberg', 'gold', 'nessie'],
    default_args=default_args
) as dag:

    sync_gold_layer = SparkSubmitOperator(
        task_id='5.execute_process_gold_layer',
        conn_id='spark_conn',
        application='/opt/airflow/dags/process_gold_layer.py',

        conf={
            "spark.master": "spark://spark-master:7077",

            # 🔥 Iceberg + Nessie
            "spark.jars.packages": (
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.7,"
                "org.apache.hadoop:hadoop-aws:3.3.4"
            ),

            # 🔥 TUNE PERFORMANCE (RẤT QUAN TRỌNG)
            "spark.sql.shuffle.partitions": "20",
            "spark.default.parallelism": "20",

            # 🔥 FIX TIMEOUT / HEARTBEAT
            "spark.network.timeout": "300s",
            "spark.executor.heartbeatInterval": "60s",

            # 🔥 MEMORY
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",

            # 🔥 Iceberg optimization
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
        },

        verbose=True  # 🔥 giúp Airflow thấy log liên tục → tránh zombie
    )

    sync_gold_layer