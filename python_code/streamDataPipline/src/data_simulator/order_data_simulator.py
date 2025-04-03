import random
import json
from faker import Faker
import pandas as pd
import os

from pandas import DataFrame
from src.utility import configuration
from src.utility.logger import logger

fake = Faker()


def fake_order(customer_df: DataFrame, product_df: DataFrame, output_type: str = "raw", delimiter: str = "|") :
    order_id = random.randint(1000, 9999)
    product_id = random.choice(product_df.iloc[:, 0].tolist())
    customer_id = random.choice(customer_df.iloc[:, 0].tolist())
    order_time = fake.date_time_this_year().isoformat()

    if output_type == "json" :
        return {
            "order_id" :order_id,
            "product_id" :product_id,
            "customer_id" :customer_id,
            "order_time" :order_time
        }
    elif output_type == "tuple" :
        return order_id, product_id, customer_id, order_time

    else :
        return f"{order_id}, {product_id}, {customer_id} {order_time}"


# Generate multiple fake products
# num_products = 4
# fake_orders = [fake_order("json") for _ in range(num_products)]

# Save to JSON file with pandas
# df = pd.DataFrame(data = fake_orders)

# df.to_json(configuration.data_dir + "fake_orders1.json", orient = "records", indent = 4)

# Print a sample product
# print(json.dumps(fake_orders[:2], indent = 4))


def generate_fake_order(customer_data_path: str, product_data_path: str, file_format: str = "tuple",
                        customer_count: int = 2, product_count: int = 5, order_count: int = 10) :
    if not os.path.exists(customer_data_path) :
        logger.info(f"File not found at {customer_data_path}")
        exit(-1)
        # logger.info(f"Creating {customer_count} seller as per seller_count configured, 2 is default")
        # customer_df = pd.DataFrame(data = [generate_fake_customer("tuple") for _ in range(customer_count)])
    else :
        if customer_data_path.endswith("json") :
            logger.info(f"Json File found at {customer_data_path}")
            customer_df = pd.read_json(customer_data_path, orient = "records")
        else :
            logger.info(f"File found at {customer_data_path}")
            customer_df = pd.read_csv(customer_data_path, sep = "|")

    if not os.path.exists(product_data_path) :
        logger.info(f"File not found at {product_data_path}")
        exit(-1)
        # logger.info(f"Creating {product_count} seller as per seller_count configured, 2 is default")
        # product_df = pd.DataFrame(data = [generate_fake_product("tuple") for _ in range(product_count)])
    else :
        if product_data_path.endswith("json") :
            logger.info(f"Json file found at {product_data_path}")
            product_df = pd.read_json(product_data_path, orient = "records")
        else :
            logger.info(f"File found at {product_data_path}")
            product_df = pd.read_csv(product_data_path, sep = "|")

    # logger.info(f"Creating {product_count} product as per product_count configured, 2 is default")
    logger.info(f"\n{customer_df}")
    logger.info(f"\n{product_df}")
    return [fake_order(customer_df = customer_df, product_df = product_df, output_type = file_format) for _ in
            range(order_count)]


result_order_5 = generate_fake_order(configuration.data_dir + "fake_custmomer1.json5",
                                     configuration.data_dir + "fake_products1.json", file_format = "json",
                                     order_count = 5)
for i in result_order_5 :
    logger.info(i)

# Creating 25 orders for testing with postgresql, generate 25 orders data created as tuple with delimiter |
from sqlalchemy import create_engine
import psycopg2

# 1. Define PostgreSQL connection details
DB_USER = "myuser"
DB_PASSWORD = "mypassword"
DB_HOST = "localhost"  # If using Docker, replace with container name e.g., "postgres_db"
DB_PORT = "5432"
DB_NAME = "ecommerce_db"

# 2. Create a connection engine
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 3. Create a sample DataFrame
order_psql = generate_fake_order(configuration.data_dir + "customer_data_7.tsv",
                                 configuration.data_dir + "product_data_12.json", order_count = 25)
order_col = ["order_id", "product_id", "customer_id", "order_time"]
df_order_psql = pd.DataFrame(data = order_psql, columns = order_col)
logger.info(f"\n{df_order_psql}")

# 4a. save DataFrame in csv file
df_order_psql.to_csv(configuration.data_dir + "order_data_25.tsv", sep = "|", index = False)
# 4b. Load DataFrame into PostgreSQL
df_order_psql.to_sql("orders", engine, if_exists = "replace", index = False)

print("Orders Data successfully loaded into PostgreSQL!")
