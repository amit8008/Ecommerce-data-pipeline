import random
import json
from faker import Faker
from utils.utility import configuration
from utils.utility.logger import logger
import pandas as pd

fake = Faker('en_IN')


def generate_fake_customer(output_type: str = "raw", delimiter: str = "|") :
    # seller_id = fake.uuid4()
    customer_id = random.randint(1000000, 9999999)
    customer_name = fake.name()
    customer_phone = fake.phone_number()
    customer_email = fake.email()
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
num_customer = 3
fake_customer = [generate_fake_customer(output_type = "json") for _ in range(num_customer)]

# Save to JSON file
# with open(configuration.data_dir + "fake_custmomer1.json", "w") as f :
#     json.dump(fake_customer, f, indent = 4)

# Print a sample product
# logger.info(json.dumps(fake_customer[:2], indent = 4))

# Generating pandas dataframe from list of tuples
fake_customer_tuple = [generate_fake_customer("tuple") for _ in range(num_customer)]
df_tuple = pd.DataFrame(data = fake_customer_tuple)
# logger.info(f"\n{df_tuple}")


# Creating 7 customer for testing with postgresql, generate 7 customer data created as tuple with delimiter |
def customer_db_ingestion():
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
    customer_psql = [generate_fake_customer("tuple") for _ in range(7)]
    customer_col = ["customer_id", "customer_name", "customer_phone", "customer_email", "customer_dob",
                    "customer_address"]
    df_customer_psql = pd.DataFrame(data = customer_psql, columns = customer_col)

    # 4a. save DataFrame in csv file
    df_customer_psql.to_csv(configuration.data_dir + "customer_data_7.tsv", sep = "|", index = False)
    # 4b. Load DataFrame into PostgreSQL
    df_customer_psql.to_sql("customer", engine, if_exists = "replace", index = False)

    print("Customer Data successfully loaded into PostgreSQL!")


# customer_db_ingestion()
