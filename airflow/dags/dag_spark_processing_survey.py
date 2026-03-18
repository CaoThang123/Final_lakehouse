from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# Định nghĩa các package cần tải từ Maven
PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.7,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

with DAG(
    dag_id='2a.Spark_Processing_Survey_Silver',
    start_date=datetime(2026, 2, 24),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'iceberg', 'nessie']
) as dag:

    process_survey_to_iceberg = SparkSubmitOperator(
        task_id='run_spark_transform',
        conn_id='spark_conn',
        application='/opt/airflow/dags/transform_survey.py',
        name='Airflow_Spark_Iceberg_With_Namespace',
        conf={
            "spark.jars.packages": PACKAGES,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
            "spark.sql.catalog.nessie.warehouse": "s3a://silver/",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        
        application_args=["survey.csv"], 
        verbose=True
    )

    process_survey_to_iceberg