import random
import json
from faker import Faker
import pandas as pd
from src.utility import configuration

fake = Faker()


def generate_fake_seller(output_type: str = "raw") :
    seller_name = fake.name().capitalize()
    # product_id = fake.uuid4()
    seller_id = random.randint(1000, 9999)
    seller_location = fake.city() + ", " + fake.country()

    if output_type == "dict" :
        return {
            "seller_id" :seller_id,
            "seller_name" :seller_name,
            "seller_location" :seller_location
        }
    elif output_type == "list" :
        return [seller_id, seller_name, seller_location]

    else :
        return f"{seller_id}, {seller_name}, {seller_location}"


# Generate multiple fake products
num_seller = 10
fake_seller = [generate_fake_seller("dict") for _ in range(num_seller)]

# Save to JSON file with pandas
df = pd.DataFrame(data = fake_seller)

df.to_json(configuration.resources_path + "fake_seller1.json", orient = "records", indent = 4)

# Print a sample product
print(json.dumps(fake_seller[:2], indent = 4))

fake_seller1 = generate_fake_seller()
print(fake_seller1)

# a = []
# d = {'col1':1, 'col2':4}
# a.append(d)
# d1 = {'col1':2, 'col2':3}
# a.append(d1)

# df = pd.DataFrame(data = a)
# df = pd.DataFrame(data = fake_seller)

# print(df)

# TODO: write a Function to generate seller data with number of seller, file formate and file path
