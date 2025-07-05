import random
import json
from faker import Faker
import pandas as pd
import os

from pandas import DataFrame

# from scr.data_simulator import seller_data_simulator
from scr.utility import configuration
from scr.utility.logger import logger

fake = Faker()

# seller_df = pd.read_json(configuration.data_dir + "fake_seller1.json", orient = 'records')
# seller_ids = seller_df['seller_id'].tolist()
# logger.debug(seller_ids)


def fake_product(seller_df: DataFrame, output_type: str = "raw", delimiter: str = "|") :
    categories = {
        "Electronics" :["Smartphone", "Laptop", "Smart TV", "Headphones", "Camera"],
        "Clothing" :["T-Shirt", "Jeans", "Jacket", "Shoes", "Dress"],
        "Home Appliances" :["Washing Machine", "Refrigerator", "Microwave", "Vacuum Cleaner"],
        "Furniture" :["Sofa", "Dining Table", "Bed", "Chair", "Wardrobe"]
    }

    category = random.choice(list(categories.keys()))
    product_name = random.choice(categories[category]) + " " + fake.word().capitalize()
    # product_id = fake.uuid4()
    product_id = random.randint(0, 5000000)
    brand = fake.company()
    price = round(random.uniform(500, 50000), 2)  # Random price between ₹500 and ₹50,000
    discount = random.choice([0, 5, 10, 15, 20, 25])  # Random discount percentage
    stock_quantity = random.randint(0, 100)  # Random stock count
    stock_status = "In Stock" if stock_quantity > 0 else "Out of Stock"
    color = random.choice(["Red", "Black", "White", "Blue", "Gray"])
    size = random.choice(["Small", "Medium", "Large"])
    weight = round(random.uniform(0.5, 15), 2)  # Random weight in kg
    material = random.choice(["Plastic", "Metal", "Wood", "Cotton", "Leather"])
    rating = round(random.uniform(1, 5), 1)  # Random rating between 1-5
    num_reviews = random.randint(0, 5000)  # Random number of reviews
    seller_id = random.choice(seller_df.iloc[:,0].tolist())
    shipping_cost = random.choice([0, 50, 100, 200])  # Free or fixed cost
    delivery_time = random.choice(["Same-day", "2-5 days", "7+ days"])
    created_date = fake.date_time_this_year().isoformat()
    last_updated = fake.date_time_this_month().isoformat()
    tags = random.sample(["Trending", "New", "Bestseller", "Budget-friendly", "Premium", "Limited Edition"],
                         random.randint(2, 4))

    if output_type == "json" :
        return {
            "product_id" :product_id,
            "product_name" :product_name,
            "category" :category,
            "brand" :brand,
            "price" :price,
            "discount" :discount,
            "stock_quantity" :stock_quantity,
            "stock_status" :stock_status,
            "color" :color,
            "size" :size,
            "weight" :f"{weight} kg",
            "material" :material,
            "rating" :rating,
            "num_reviews" :num_reviews,
            "seller_id" :seller_id,
            "shipping_cost" :shipping_cost,
            "delivery_time" :delivery_time,
            "created_date" :created_date,
            "last_updated" :last_updated,
            "tags" :tags
        }
    elif output_type == "tuple" :
        return (product_id, product_name, category, brand, price, discount, stock_quantity, stock_status, color, size,
                weight, material, rating, num_reviews, seller_id, shipping_cost,
                delivery_time, created_date, last_updated, tags)

    else :
        return f"{product_id}{delimiter}{product_name}{delimiter}{category}{delimiter}{brand}{delimiter}{price}{delimiter}{discount}{delimiter}{stock_quantity}{delimiter}{stock_status}{delimiter}{color}{delimiter}{size}{delimiter}{weight}{delimiter}{material}{delimiter}{rating}{delimiter}{num_reviews}{delimiter}{seller_id}{delimiter}{shipping_cost}{delimiter}{delivery_time}{delimiter}{created_date}{delimiter}{last_updated}{delimiter}{",".join(tags)}"


# Generate multiple fake products
# num_products = 4
# fake_products = [fake_product(output_type = "json") for _ in range(num_products)]

# Save to JSON file
# df = pd.DataFrame(data = fake_products)

# df.to_json(configuration.data_dir + "fake_products1.json", orient = "records", indent = 4)

# Print a sample product
# logger.info(json.dumps(fake_products[:2], indent = 4))


def generate_fake_product(seller_data_path: str, seller_count: int = 2, product_count: int = 5, file_format: str = "json") :
    if not os.path.exists(seller_data_path) :
        logger.info(f"File not found at {seller_data_path}")
        # logger.info(f"Creating {seller_count} seller as per seller_count configured, 2 is default")
        # seller_df = pd.DataFrame(data = [seller_data_simulator.generate_fake_seller("tuple") for _ in range(seller_count)])
    else:
        if seller_data_path.endswith("json"):
            logger.info(f"File found at {seller_data_path}")
            seller_df = pd.read_json(seller_data_path,  orient = "records")
        else:
            logger.info(f"File found at {seller_data_path}")
            seller_df = pd.read_csv(seller_data_path,  sep = "|")

    logger.info(f"Creating {product_count} product as per product_count configured, 5 is default")
    logger.info(f"\n{seller_df}")
    return [fake_product(seller_df = seller_df, output_type = file_format) for _ in range(product_count)]


# result_2_5 = generate_fake_product("")
# logger.info(result_2_5)


result_json_5 = generate_fake_product(configuration.data_dir + "fake_seller1.json")
# logger.info(result_json_5)


# Creating 12 product for testing with postgresql, generate 12 product data created as tuple with delimiter |
def product_db_ingestion():
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
    product_psql = generate_fake_product(configuration.data_dir + "seller_data_5.tsv", product_count = 12)
    df_product_psql = pd.DataFrame(data = product_psql)
    logger.info(f"\n{df_product_psql}")

    # 4a. save DataFrame in csv file
    df_product_psql.to_json(configuration.data_dir + "product_data_12.json", orient = "records", indent = 4)
    # 4b. Load DataFrame into PostgreSQL
    df_product_psql.to_sql("product", engine, if_exists = "replace", index = False)

    print("Product Data successfully loaded into PostgreSQL!")


# product_db_ingestion()


