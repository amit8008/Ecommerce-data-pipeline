import random
import json
from faker import Faker

fake = Faker()


def generate_fake_seller(output_type: str = "raw"):
    seller_name = fake.name().capitalize()
    # product_id = fake.uuid4()
    seller_id = random.randint(1000, 9999)
    seller_location = fake.city() + ", " + fake.country()

    if output_type == "dict":
        return {
            "seller_id" :seller_id,
            "seller_name" :seller_name,
            "seller_location" :seller_location
        }
    elif output_type == "list":
        return [seller_id, seller_name, seller_location]

    else:
        return f"{seller_id}, {seller_name}, {seller_location}"


# Generate multiple fake products
num_products = 3
fake_seller = [generate_fake_seller("dict") for _ in range(num_products)]

# Save to JSON file
with open(
        "C:\\Users\\Public\\Documents\\Stream-data-pipelines\\python_code\\streamDataPipline\\resources\\fake_seller1.json",
        "w") as f :
    json.dump(fake_seller, f, indent = 4)

# Print a sample product
print(json.dumps(fake_seller[:2], indent = 4))

fake_seller1 = generate_fake_seller()
print(fake_seller1)

