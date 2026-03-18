from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='2b.Kafka_to_Silver_Purchase',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    SparkSubmitOperator(
        task_id='execute_streaming_script',
        conn_id='spark_conn',
        application='/opt/airflow/dags/process_purchase_kafka.py',
        conf={
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.7,org.apache.hadoop:hadoop-aws:3.3.4"
        }
    )