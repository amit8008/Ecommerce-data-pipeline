import random
import json
from faker import Faker
import pandas as pd
from src.utility import configuration
from src.utility.logger import logger

fake = Faker()

seller_df = pd.read_json(configuration.resources_path + "fake_seller1.json", orient = 'records')
seller_ids = seller_df['seller_id'].tolist()
# logger.debug(seller_ids)


def generate_fake_product(output_type: str = "raw") :
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
    seller_id = random.choice(seller_ids)
    shipping_cost = random.choice([0, 50, 100, 200])  # Free or fixed cost
    delivery_time = random.choice(["Same-day", "2-5 days", "7+ days"])
    created_date = fake.date_time_this_year().isoformat()
    last_updated = fake.date_time_this_month().isoformat()
    tags = random.sample(["Trending", "New", "Bestseller", "Budget-friendly", "Premium", "Limited Edition"],
                         random.randint(2, 4))

    if output_type == "dict" :
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
    elif output_type == "list" :
        return [product_id, product_name, category, brand, price, discount, stock_quantity, stock_status, color, size,
                weight, material, rating, num_reviews, seller_id, shipping_cost,
                delivery_time, created_date, last_updated, tags]

    else :
        return f"{product_id}, {product_name}, {category}, {brand}, {price}, {discount}, {stock_quantity}, {stock_status}, {color}, {size}, {weight}, {material}, {rating}, {num_reviews}, {seller_id}, {shipping_cost}, {delivery_time}, {created_date}, {last_updated}, {"|".join(tags)}"


# Generate multiple fake products
num_products = 3
fake_products = [generate_fake_product("dict") for _ in range(num_products)]

# Save to JSON file
df = pd.DataFrame(data = fake_products)

df.to_json(configuration.resources_path + "fake_products1.json", orient = "records", indent = 4)

# Print a sample product
logger.info(json.dumps(fake_products[:2], indent = 4))

