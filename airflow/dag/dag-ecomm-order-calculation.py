from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import boto3
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
}

# Constants for S3 bucket and file paths
BUCKET_NAME = 'ecomm-airflow-bucket'
CUSTOMER_FILE_PATH = 'ecomm_data/customer_175.json'
PRODUCT_FILE_PATH = 'ecomm_data/product_500.json'
SELLER_FILE_PATH = 'ecomm_data/seller_30.json'
ORDER_PREFIX = 'ecomm_data/streams/'
ARCHIVE_PREFIX = 'ecomm_data/streams/archived/'

REQUIRED_COLUMNS = {
    'customer' :['customer_id', 'customer_name', 'customer_phone', 'customer_email', 'customer_dob',
                 'customer_address'],
    'product' :['product_id', 'product_name', 'category', 'brand', 'price', 'discount', 'stock_quantity',
                'stock_status', 'color', 'size', 'weight', 'material', 'rating', 'num_reviews', 'seller_id',
                'shipping_cost', 'delivery_time', 'created_date', 'last_updated', 'tags'],
    'seller' :['seller_id', 'seller_name', 'seller_location'],
    'order' :['order_id', 'product_id', 'customer_id', 'order_time']
}


def list_s3_files(prefix, bucket=BUCKET_NAME) :
    """ List all files in S3 bucket that match the prefix """
    s3 = boto3.client('s3')
    try :
        response = s3.list_objects_v2(Bucket = bucket, Prefix = prefix)
        files = [content['Key'] for content in response.get('Contents', []) if
                 content['Key'].endswith(('.csv', '.json'))]
        logging.info(f"Successfully listed files with prefix {prefix} from S3: {files}")
        return files
    except Exception as e :
        logging.error(f"Failed to list files with prefix {prefix} from S3: {str(e)}")
        raise Exception(f"Failed to list files with prefix {prefix} from S3: {str(e)}")


def read_s3_csv(file_name, bucket=BUCKET_NAME) :
    """ Helper function to read a CSV file from S3 """
    s3 = boto3.client('s3')
    try :
        obj = s3.get_object(Bucket = bucket, Key = file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_csv(obj['Body'])
    except Exception as e :
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")


def read_s3_json(file_name, bucket=BUCKET_NAME) :
    """ Helper function to read a JSON file from S3 """
    s3 = boto3.client('s3')
    try :
        obj = s3.get_object(Bucket = bucket, Key = file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_json(obj['Body'])
    except Exception as e :
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")


def validate_datasets() :
    validation_results = {}

    # Validate customer dataset
    try :
        customer_data = read_s3_json(CUSTOMER_FILE_PATH)
        missing_columns = set(REQUIRED_COLUMNS['customer']) - set(customer_data.columns)
        if not missing_columns :
            validation_results['customer'] = True
            logging.info("All required columns present in customer")
        else :
            validation_results['customer'] = False
            logging.warning(f"Missing columns in customer: {missing_columns}")
    except Exception as e :
        validation_results['customer'] = False
        logging.error(f"Failed to read or validate customer from S3: {e}")
        raise

    # Validate product dataset
    try :
        product_data = read_s3_json(PRODUCT_FILE_PATH)
        missing_columns = set(REQUIRED_COLUMNS['product']) - set(product_data.columns)
        if not missing_columns :
            validation_results['product'] = True
            logging.info("All required columns present in product")
        else :
            validation_results['product'] = False
            logging.warning(f"Missing columns in product: {missing_columns}")
    except Exception as e :
        validation_results['product'] = False
        logging.error(f"Failed to read or validate product from S3: {e}")
        raise

    # Validate seller dataset
    try :
        seller_data = read_s3_json(SELLER_FILE_PATH)
        missing_columns = set(REQUIRED_COLUMNS['seller']) - set(seller_data.columns)
        if not missing_columns :
            validation_results['seller'] = True
            logging.info("All required columns present in seller")
        else :
            validation_results['seller'] = False
            logging.warning(f"Missing columns in seller: {missing_columns}")
    except Exception as e :
        validation_results['seller'] = False
        logging.error(f"Failed to read or validate seller from S3: {e}")
        raise

    # Validate orders datasets
    try :
        order_files = list_s3_files(ORDER_PREFIX)
        for order_file in order_files :
            streams_data = read_s3_csv(order_file)
            missing_columns = set(REQUIRED_COLUMNS['order']) - set(streams_data.columns)
            if not missing_columns :
                validation_results['order'] = True
                logging.info(f"All required columns present in {order_file}")
            else :
                validation_results['streams'] = False
                logging.warning(f"Missing columns in {order_file}: {missing_columns}")
                break
    except Exception as e :
        validation_results['order'] = False
        logging.error(f"Failed to read or validate order from S3: {e}")
        raise

    return validation_results


def branch_task(ti) :
    validation_results = ti.xcom_pull(task_ids = 'validate_datasets')

    if all(validation_results.values()) :
        return 'top_selling_category'
    else :
        return 'end_dag'


def upsert_to_redshift(df, table_name, id_columns) :
    redshift_hook = PostgresHook(postgres_conn_id = "redshift_default")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    try :
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Create insert query for the temporary table
        cols = ', '.join(list(df.columns))
        vals = ', '.join(['%s'] * len(df.columns))
        tmp_table_query = f"INSERT INTO reporting_schema.tmp_{table_name} ({cols}) VALUES ({vals})"

        cursor.executemany(tmp_table_query, data_tuples)

        # Create the merge (upsert) query
        delete_condition = ' AND '.join([f'tmp_{table_name}.{col} = {table_name}.{col}' for col in id_columns])
        merge_query = f"""
        BEGIN;
        DELETE FROM reporting_schema.{table_name}
        USING reporting_schema.tmp_{table_name}
        WHERE {delete_condition};

        INSERT INTO reporting_schema.{table_name}
        SELECT * FROM reporting_schema.tmp_{table_name};

        TRUNCATE TABLE reporting_schema.tmp_{table_name};
        COMMIT;
        """

        cursor.execute(merge_query)
        conn.commit()
        logging.info(f"Data ingested and merged successfully into {table_name}")
    except Exception as e :
        conn.rollback()
        logging.error(f"Failed to ingest and merge data into {table_name}: {e}")
        raise
    finally :
        cursor.close()
        conn.close()


def top_selling_category() :
    order_files = list_s3_files(ORDER_PREFIX)
    orders_data = pd.concat([read_s3_csv(file) for file in order_files], ignore_index = True)
    product_data = read_s3_csv(PRODUCT_FILE_PATH)

    merged_data = orders_data.merge(product_data, on = 'product_id', how = 'left')
    category_count = merged_data.groupby('category').size().reset_index(name = 'category_count')

    logging.info("Category-level KPIs:")
    logging.info(category_count.columns)

    upsert_to_redshift(category_count, 'category_count', ['category'])


def top_10_customer() :
    order_files = list_s3_files(ORDER_PREFIX)
    orders_data = pd.concat([read_s3_csv(file) for file in order_files], ignore_index = True)
    customer_data = read_s3_csv(CUSTOMER_FILE_PATH)

    merged_data = orders_data.merge(customer_data, left_on = 'customer_id')
    top_10_cust = (merged_data.groupby('customer_id')
                   .agg(total_orders = ('order_id', 'count'))
                   .reset_index()
                   .sort_values(by = 'total_orders', ascending = False)
                   .head(10)
                   )

    logging.info("Top 10 customers: ")
    logging.info(top_10_cust.columns)

    upsert_to_redshift(top_10_cust, 'top_10_cust', ['customer_id'])


def move_processed_files() :
    s3 = boto3.client('s3')
    try :
        stream_files = list_s3_files(ORDER_PREFIX)
        for file in stream_files :
            copy_source = {'Bucket' :BUCKET_NAME, 'Key' :file}
            destination_key = file.replace('ecomm_data/streams/', 'ecomm_data/streams/archived/')
            s3.copy_object(CopySource = copy_source, Bucket = BUCKET_NAME, Key = destination_key)
            s3.delete_object(Bucket = BUCKET_NAME, Key = file)
            logging.info(f"Moved {file} to {destination_key}")
    except Exception as e :
        logging.error(f"Failed to move files from {ORDER_PREFIX} to {ARCHIVE_PREFIX}: {str(e)}")
        raise


with DAG('data_validation_and_order_computation', default_args = default_args, schedule_interval = '@daily') as dag :
    validate_datasets = PythonOperator(
        task_id = 'validate_datasets',
        python_callable = validate_datasets
    )

    check_validation = BranchPythonOperator(
        task_id = 'check_validation',
        python_callable = branch_task,
        provide_context = True
    )

    top_selling_category = PythonOperator(
        task_id = 'top_selling_category',
        python_callable = top_selling_category
    )

    top_10_customer = PythonOperator(
        task_id = 'top_10_customer',
        python_callable = top_10_customer
    )

    move_files = PythonOperator(
        task_id = 'move_processed_files',
        python_callable = move_processed_files
    )

    end_dag = DummyOperator(
        task_id = 'end_dag'
    )

    validate_datasets >> check_validation >> [top_selling_category, end_dag]
    top_selling_category >> top_10_customer >> move_files
