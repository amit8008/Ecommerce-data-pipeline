from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def ingest_sales_data() :
    print("Ingesting and clean order data into Iceberg tables.")


with DAG(
        dag_id = 'order_ingestion_dag',
        start_date = datetime(2025, 10, 1),
        schedule_interval = '@daily',
        catchup = False,
        tags = ['bronze', 'order']
) as dag :

    ingest_to_bronze = SparkSubmitOperator(
        task_id = 'ingest_order_to_bronze',
        application = '/ecom/libs/Stream_data_pipelines-assembly-0.1.0-SNAPSHOT.jar',
        conn_id = 'spark_docker_connection',
        jars = "/ecom/libs/mysql-connector-j-8.0.33.jar,/ecom/libs/postgresql-42.2.23.jar",
        java_class = "com.ecommerce.extract.IngestOrderBronze"
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id = 'load_order_bronze_to_silver',
        application = '/ecom/libs/Stream_data_pipelines-assembly-0.1.0-SNAPSHOT.jar',
        conn_id = 'spark_docker_connection',
        jars = "/ecom/libs/mysql-connector-j-8.0.33.jar,/ecom/libs/postgresql-42.2.23.jar",
        java_class = "com.ecommerce.load.LoadOrderSilver"
    )

    ingest_to_bronze >> bronze_to_silver
