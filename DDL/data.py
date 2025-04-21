from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from faker import Faker
import uuid
from datetime import datetime, timedelta
import random

# Initialize Faker
fake = Faker()

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra host
session = cluster.connect()

# Use the ecommerce keyspace
session.set_keyspace('ecommerce')

# Helper function to generate random timestamps
def random_timestamp(start, end):
    return fake.date_time_between(start_date=start, end_date=end)

# Populate users tables
def populate_users():
    for _ in range(50):
        user_id = uuid.uuid4()
        email = fake.email()
        name = fake.name()
        last_name = name.split()[-1]

        session.execute("INSERT INTO users_by_id (user_id, email, name) VALUES (%s, %s, %s)", (user_id, email, name))
        session.execute("INSERT INTO users_by_email (email, user_id, name) VALUES (%s, %s, %s)", (email, user_id, name))
        session.execute("INSERT INTO users_by_last_name (last_name, user_id, email, name) VALUES (%s, %s, %s, %s)", (last_name, user_id, email, name))

# Populate products tables
category_products = {
    'Electronics': {
        'Wireless Mouse': 29.99,
        'Bluetooth Speaker': 49.99,
        'Noise Cancelling Headphones': 119.99,
        'Smartphone Charger': 19.99,
        'LED Monitor': 199.99,
        'Portable SSD 1TB': 89.50,
        'Mechanical Keyboard': 74.25
    },
    'Books': {
        'The Art of War': 12.99,
        'Python Programming Basics': 35.50,
        'Mystery of the Old House': 9.99,
        'Journey to the Unknown': 14.99,
        'Tales of the Future': 11.49,
        'Cassandra for Beginners': 27.99,
        'Deep Work': 21.00
    },
    'Clothing': {
        'Classic White T-Shirt': 15.99,
        'Slim Fit Jeans': 45.00,
        'Leather Jacket': 149.99,
        'Running Shoes': 79.99,
        'Wool Sweater': 59.95,
        'Denim Skirt': 39.50,
        'Cotton Hoodie': 34.25
    },
    'Home': {
        'Ceramic Vase': 24.99,
        'LED Desk Lamp': 32.49,
        'Memory Foam Pillow': 44.99,
        'Essential Oil Diffuser': 29.99,
        'Cast Iron Skillet': 55.00,
        'Wall Clock': 19.95,
        'Minimalist Bookshelf': 120.00
    },
    'Toys': {
        'Lego Star Wars Set': 89.99,
        'Wooden Train Set': 39.95,
        'Stuffed Panda': 18.50,
        'Remote Control Car': 49.90,
        'Puzzle Cube 3x3': 9.99,
        'Board Game: Catan': 44.00,
        'Plush Dinosaur': 22.00
    }
}

def populate_products():
    for category, products in category_products.items():
        for name, price in products.items():
            product_id = uuid.uuid4()
            description = fake.sentence(nb_words=10)  # still a lil' random
            session.execute(
                "INSERT INTO products_by_id (product_id, name, description, price, category) VALUES (%s, %s, %s, %s, %s)",
                (product_id, name, description, price, category)
            )
            session.execute(
                "INSERT INTO products_by_category (category, product_id, name, price) VALUES (%s, %s, %s, %s)",
                (category, product_id, name, price)
            )
 

# Populate orders tables
def populate_orders():
    for _ in range(30):
        user_id = uuid.uuid4()
        order_id = uuid.uuid4()
        order_date = random_timestamp('-1y', 'now')
        total = round(random.uniform(20, 1000), 2)

        session.execute("INSERT INTO orders_by_user_id (user_id, order_id, order_date, total) VALUES (%s, %s, %s, %s)", (user_id, order_id, order_date, total))

        # Add order items
        for _ in range(random.randint(1, 5)):
            product_id = uuid.uuid4()
            quantity = random.randint(1, 10)
            price_at_purchase = round(random.uniform(10, 500), 2)

            session.execute("INSERT INTO order_items_by_order_id (order_id, product_id, quantity, price_at_purchase) VALUES (%s, %s, %s, %s)", (order_id, product_id, quantity, price_at_purchase))

# Populate reviews table
def populate_reviews():
    for _ in range(50):
        product_id = uuid.uuid4()
        review_id = uuid.uuid4()
        user_id = uuid.uuid4()
        rating = random.randint(1, 5)
        review = fake.text(max_nb_chars=200)
        created_at = random_timestamp('-1y', 'now')

        session.execute("INSERT INTO reviews_by_product (product_id, review_id, user_id, rating, review, created_at) VALUES (%s, %s, %s, %s, %s, %s)", (product_id, review_id, user_id, rating, review, created_at))

# Populate cart table
def populate_cart():
    for _ in range(30):
        user_id = uuid.uuid4()
        for _ in range(random.randint(1, 5)):
            product_id = uuid.uuid4()
            quantity = random.randint(1, 10)
            added_at = random_timestamp('-1y', 'now')

            session.execute("INSERT INTO cart_by_user (user_id, product_id, quantity, added_at) VALUES (%s, %s, %s, %s)", (user_id, product_id, quantity, added_at))

# Populate product views table
def populate_product_views():
    for _ in range(100):
        product_id = uuid.uuid4()
        view_timestamp = random_timestamp('-1y', 'now')

        session.execute("INSERT INTO product_views (product_id, view_timestamp) VALUES (%s, %s)", (product_id, view_timestamp))

# Populate shipment table
def populate_shipments():
    for _ in range(30):
        order_id = uuid.uuid4()
        status = random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled'])
        shipped_at = random_timestamp('-1y', 'now') if status in ['Shipped', 'Delivered'] else None
        estimated_arrival = shipped_at + timedelta(days=random.randint(1, 10)) if shipped_at else None
        tracking_number = fake.uuid4() if status in ['Shipped', 'Delivered'] else None

        session.execute("INSERT INTO shipment_by_order (order_id, status, shipped_at, estimated_arrival, tracking_number) VALUES (%s, %s, %s, %s, %s)", (order_id, status, shipped_at, estimated_arrival, tracking_number))

# Main function to populate all tables
def main():
    populate_users()
    populate_products()
    populate_orders()
    populate_reviews()
    populate_cart()
    populate_product_views()
    populate_shipments()
    print("Data population completed.")

if __name__ == "__main__":
    main()