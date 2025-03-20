import random
import json
from faker import Faker
import pandas as pd

fake = Faker()

resource_loc = "C:\\Users\\Public\\Documents\\Stream-data-pipelines\\python_code\\streamDataPipline\\resources\\"

# Load product, customer
product_df = pd.read_json(resource_loc + "fake_products1.json", orient = 'records')
product_ids = product_df['product_id'].tolist()
# print(product_ids)

customer_df = pd.read_json(resource_loc + "fake_custmomer1.json", orient = 'records')
customer_ids = customer_df['customer_id'].tolist()


# print(customer_ids)

def generate_fake_order(output_type: str = "raw") :
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
num_products = 3
fake_orders = [generate_fake_order("dict") for _ in range(num_products)]

# Save to JSON file with pandas
df = pd.DataFrame(data = fake_orders)

df.to_json(resource_loc + "fake_orders1.json", orient = "records", indent = 4)

# Print a sample product
print(json.dumps(fake_orders[:2], indent = 4))
