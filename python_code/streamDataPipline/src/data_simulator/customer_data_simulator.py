import random
import json
from faker import Faker
from src.utility import configuration
from src.utility.logger import logger

fake = Faker()


def generate_fake_customer(output_type: str = "raw") :
    # seller_id = fake.uuid4()
    customer_id = random.randint(1000000, 9999999)
    customer_name = fake.name()
    customer_dob = fake.date_of_birth(minimum_age = 10, maximum_age = 80).isoformat()
    customer_address = fake.address()

    if output_type == "dict" :
        return {
            "customer_id" :customer_id,
            "customer_name" :customer_name,
            "customer_dob" :customer_dob,
            "customer_address" :customer_address
        }
    elif output_type == "list" :
        return [customer_id, customer_name, customer_dob, customer_address]
    else :
        return f"{customer_id}, {customer_name}, {customer_dob}, {customer_address}"


# Generate multiple fake products
num_products = 3
fake_customer = [generate_fake_customer("dict") for _ in range(num_products)]

# Save to JSON file
with open(configuration.data_dir + "fake_custmomer1.json", "w") as f :
    json.dump(fake_customer, f, indent = 4)

# Print a sample product
logger.debug(json.dumps(fake_customer[:2], indent = 4))

fake_customer1 = generate_fake_customer()
logger.info(fake_customer1)
