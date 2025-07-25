import random
import json
from faker import Faker
import pandas as pd
from scr.utility import configuration
from scr.utility.logger import logger

fake = Faker('en_IN')


def generate_fake_seller(output_type: str = "raw", delimiter="|") :
    seller_name = fake.name().capitalize()
    seller_id = random.randint(1000, 9999)
    seller_location = fake.address().replace("\n", ",")

    if output_type == "json" :
        return {
            "seller_id" :seller_id,
            "seller_name" :seller_name,
            "seller_location" :seller_location
        }
    elif output_type == "tuple" :
        return seller_id, seller_name, seller_location

    else :
        return f"{seller_id}{delimiter}{seller_name}{delimiter}{seller_location}"


# Generate multiple fake products
num_seller = 3

# Save to JSON file with pandas
fake_seller_json = [generate_fake_seller(output_type = "json") for _ in range(30)]
df_json = pd.DataFrame(data = fake_seller_json)
# df_json.to_json(configuration.data_dir + "seller_30.json", orient = "records", indent = 4)
print(df_json.columns)

# Print a sample product
# logger.debug(json.dumps(fake_seller_json[:2], indent = 4))

# Generating pandas dataframe from list of tuples
# fake_seller_tuple = [generate_fake_seller("tuple") for _ in range(30)]
# df_tuple = pd.DataFrame(data = fake_seller_tuple)
# df_tuple.to_json(configuration.data_dir + "seller_30.json", orient = "records", indent = 4)


# logger.debug(f"\n{df_tuple}")
# fake_seller1 = generate_fake_seller("tuple")
# logger.debug(f"{fake_seller1[0]} | {fake_seller1[1]} | {fake_seller1[2]}")

# Generating tab delimited airflow
# fake_seller2 = generate_fake_seller()
# logger.debug(f"{fake_seller2}")

# Creating 5 seller for testing with postgresql, generate 5 seller airflow created as tuple with delimiter |
def seller_db_ingestion() :
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
    seller_psql = [generate_fake_seller("tuple") for _ in range(5)]
    df_seller_psql = pd.DataFrame(data = seller_psql, columns = ["seller_id", "seller_name", "seller_location"])

    # 4a. save DataFrame in csv file
    df_seller_psql.to_csv(configuration.data_dir + "seller_data_5.tsv", sep = "|", index = False)
    # 4b. Load DataFrame into PostgreSQL
    df_seller_psql.to_sql("seller_bronze", engine, if_exists = "append", index = False)

    logger.info("Seller Data successfully loaded into PostgreSQL!")

# seller_db_ingestion()

# a = []
# d = {'col1':1, 'col2':4}
# a.append(d)
# d1 = {'col1':2, 'col2':3}
# a.append(d1)

# df = pd.DataFrame(airflow = a)
# df = pd.DataFrame(airflow = fake_seller)

# print(df)

# TODO: write a Function to generate seller airflow with number of seller, file format and file path
