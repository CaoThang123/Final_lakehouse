from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow"}
#11
with DAG(
    dag_id='2b.bronze_to_silver_purchase',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    # --- TASK : Silver ---
    silver_task = SparkSubmitOperator(
        task_id='run_silver_stream',
        conn_id='spark_conn',
        application='/opt/airflow/dags/silver_stream.py',
        conf={
            "spark.jars.packages":
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.7,"
                "org.apache.hadoop:hadoop-aws:3.3.4",
        }
    )

    silver_task