from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


def aggregate_gold():
    print("Aggregating top 5 customers from silver tables of customer and order -> gold.top_5_customers")


with DAG(
        dag_id = 'top_5_customer_gold_transformation_dag',
        start_date = datetime(2025, 10, 1),
        schedule_interval = '@daily',
        catchup = False
) as dag:
    wait_for_customer = ExternalTaskSensor(
        task_id = 'wait_for_customer_ingestion',
        external_dag_id = 'customer_ingestion_dag',
        external_task_id = 'load_customer_bronze_to_silver',  # waits until this task succeeds
        poke_interval = 60,
        timeout = 3600,
        mode = 'reschedule'
    )

    wait_for_sales = ExternalTaskSensor(
        task_id = 'wait_for_sales_ingestion',
        external_dag_id = 'sales_ingestion_dag',
        external_task_id = 'load_order_bronze_to_silver',
        poke_interval = 60,
        timeout = 3600,
        mode = 'reschedule'
    )

    aggregate = SparkSubmitOperator(
        task_id = 'aggregate_top_5_customers',
        application = '/ecom/libs/Stream_data_pipelines-assembly-0.1.0-SNAPSHOT.jar',
        conn_id = 'spark_docker_connection',
        jars = "/ecom/libs/mysql-connector-j-8.0.33.jar,/ecom/libs/postgresql-42.2.23.jar",
        java_class = "com.ecommerce.transform.Top5Customer"
    )

    [wait_for_sales, wait_for_customer] >> aggregate


