import random
import json
from faker import Faker
from scr.utility import configuration
from scr.utility.logger import logger
import pandas as pd

fake = Faker('en_IN')


def generate_fake_customer(output_type: str = "raw", delimiter: str = "|") :
    # seller_id = fake.uuid4()
    customer_id = random.randint(1000000, 9999999)
    customer_name = fake.name()
    customer_phone = fake.phone_number()
    customer_email = f'{customer_name.replace(" ", "_")}@example.org'
    customer_dob = fake.date_of_birth(minimum_age = 10, maximum_age = 80).isoformat()
    customer_address = fake.address().replace("\n", ",")

    if output_type == "json" :
        return {
            "customer_id" :customer_id,
            "customer_name" :customer_name,
            "customer_phone": customer_phone,
            "customer_email": customer_email,
            "customer_dob" :customer_dob,
            "customer_address" :customer_address
        }
    elif output_type == "tuple" :
        return customer_id, customer_name, customer_phone, customer_email,customer_dob, customer_address
    else :
        return f"{customer_id}{delimiter}{customer_name}{delimiter}{customer_phone}{delimiter}{customer_email}{delimiter}{customer_dob}{delimiter}{customer_address}"


# Generate multiple fake customers
# num_customer = 3
# fake_customer = [generate_fake_customer(output_type = "json") for _ in range(num_customer)]

# Save to JSON file
# with open(configuration.data_dir + "fake_custmomer1.json", "w") as f :
#     json.dump(fake_customer, f, indent = 4)

# Print a sample product
# logger.info(json.dumps(fake_customer[:2], indent = 4))

# Generating pandas dataframe from list of tuples
fake_customer = [generate_fake_customer("json") for _ in range(175)]
df_json = pd.DataFrame(data = fake_customer)
# logger.info(f"\n{df_json}")
df_json.to_json(configuration.data_dir + "customer_175.json", orient = "records", indent = 4)

