from scr.data_simulator.customer_data_simulator import generate_fake_customer
import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def customer_db_ingestion(count, db_user,db_password,db_host,db_port="5432",db_name="ecommerce"):

    """
    :param count: Number of customer airflow need to ingest in table
    :param db_user: database user
    :param db_password: database password
    :param db_host: database host, If using Docker, replace with container name e.g., "postgres_db"
    :param db_port: port
    :param db_name: database name
    :return: None
    """

    # Create a connection engine
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    # Create a DataFrame with fake customer generator
    customer_psql = [generate_fake_customer("tuple") for _ in range(count)]
    customer_col = ["customer_id", "customer_name", "customer_phone", "customer_email", "customer_dob",
                    "customer_address"]
    df_customer_psql = pd.DataFrame(data = customer_psql, columns = customer_col)

    # Load DataFrame into PostgresSQL
    df_customer_psql.to_sql("customer", engine, if_exists = "append", index = False)

    print(f"{count} customer airflow successfully loaded into PostgresSQL!")

# customer_db_ingestion(200, "amitsingh", "amitsingh123", "host.docker.internal")
