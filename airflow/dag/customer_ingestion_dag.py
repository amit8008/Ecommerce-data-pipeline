from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

def ingest_customer_data():
    print("Extracting and loading customer data into bronze Iceberg table.")


with DAG(
    dag_id='customer_ingestion_dag',
    start_date=datetime(2025, 10, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['bronze', 'customer']
) as dag:

    extract_to_bronze = SparkSubmitOperator(
        task_id = 'extract_customer_to_bronze',
        application = '/ecom/libs/Stream_data_pipelines-assembly-0.1.0-SNAPSHOT.jar',
        conn_id = 'spark_docker_connection',
        jars = "/ecom/libs/mysql-connector-j-8.0.33.jar,/ecom/libs/postgresql-42.2.23.jar",
        java_class = "com.ecommerce.extract.ExtractCustomerBronze"
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id = 'load_customer_bronze_to_silver',
        application = '/ecom/libs/Stream_data_pipelines-assembly-0.1.0-SNAPSHOT.jar',
        conn_id = 'spark_docker_connection',
        jars = "/ecom/libs/mysql-connector-j-8.0.33.jar,/ecom/libs/postgresql-42.2.23.jar",
        java_class = "com.ecommerce.load.LoadCustomerSilver"
    )

    extract_to_bronze >> bronze_to_silver
