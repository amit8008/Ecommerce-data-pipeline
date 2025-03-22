import random
import json
from faker import Faker
import pandas as pd
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
    product_id = random.choice(product_ids)
    customer_id = random.choice(customer_ids)
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


# def generate_fake_order(customer_data_path: str, product_data_path: str, customer_count: int = 2, product_count:int = 5,order_count:int = 10):
#