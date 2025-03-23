import random
import json
from faker import Faker
import pandas as pd
import os

from src.data_simulator.customer_data_simulator import generate_fake_customer
from src.data_simulator.product_data_simulator import generate_fake_product
from src.utility import configuration
from src.utility.logger import logger

fake = Faker()

# Load product, customer
product_df = pd.read_json(configuration.data_dir + "fake_products1.json", orient = 'records')
product_ids = product_df['product_id'].tolist()
# logger.debug(product_ids)

customer_df = pd.read_json(configuration.data_dir + "fake_custmomer1.json", orient = 'records')
customer_ids = customer_df['customer_id'].tolist()


# logger.debug(customer_ids)

def fake_order(output_type: str = "raw", delimiter: str = "|") :
    order_id = random.randint(1000, 9999)
    product_id = random.choice(product_ids)  # need update
    customer_id = random.choice(customer_ids) # need update
    order_time = fake.date_time_this_year().isoformat()

    if output_type == "dict" :
        return {
            "order_id" :order_id,
            "product_id" :product_id,
            "customer_id" :customer_id,
            "order_time" :order_time
        }
    elif output_type == "list" :
        return [order_id, product_id, customer_id, order_time]

    else :
        return f"{order_id}, {product_id}, {customer_id} {order_time}"


# Generate multiple fake products
num_products = 4
fake_orders = [fake_order("dict") for _ in range(num_products)]

# Save to JSON file with pandas
df = pd.DataFrame(data = fake_orders)

df.to_json(configuration.data_dir + "fake_orders1.json", orient = "records", indent = 4)

# Print a sample product
print(json.dumps(fake_orders[:2], indent = 4))


def generate_fake_order(customer_data_path: str, product_data_path: str, customer_count: int = 2, product_count:int = 5,order_count:int = 10):
    if not os.path.exists(customer_data_path) :
        logger.info(f"File not found at {customer_data_path}")
        logger.info(f"Creating {customer_count} seller as per seller_count configured, 2 is default")
        customer_df = pd.DataFrame(data = [generate_fake_customer("tuple") for _ in range(customer_count)])
    else:
        if customer_data_path.endswith("json"):
            logger.info(f"File found at {customer_data_path}")
            customer_df = pd.read_json(customer_data_path,  orient = "records")
        else:
            logger.info(f"File found at {customer_data_path}")
            customer_df = pd.read_csv(customer_data_path,  sep = "|")

    if not os.path.exists(product_data_path) :
        logger.info(f"File not found at {product_data_path}")
        logger.info(f"Creating {product_count} seller as per seller_count configured, 2 is default")
        product_df = pd.DataFrame(data = [generate_fake_product("tuple") for _ in range(product_count)])
    else:
        if customer_data_path.endswith("json"):
            logger.info(f"File found at {product_data_path}")
            product_df = pd.read_json(product_data_path,  orient = "records")
        else:
            logger.info(f"File found at {product_data_path}")
            product_df = pd.read_csv(product_data_path,  sep = "|")


    # logger.info(f"Creating {product_count} product as per product_count configured, 2 is default")
    # logger.info(f"\n{seller_df}")
    # return [fake_order(seller_df = seller_df) for _ in range(product_count)]