from cassandra.cluster import Cluster
from faker import Faker
import uuid
from datetime import datetime, timedelta
import random

fake = Faker()
cluster = Cluster(['localhost'])
session = cluster.connect('ecommerce')


def generate_sample_data():
    product_categories = ['electronics', 'books', 'clothing', 'home', 'toys']
    sample_products = []
    sample_orders = []
    sample_reviews = []
    sample_cart_items = []
    sample_views = []
    sample_shipments = []
    sample_users = []


    for _ in range(10): 
        product_id = uuid.uuid4()
        category = random.choice(product_categories)
        name = fake.word().capitalize()
        description = fake.sentence()
        price = round(random.uniform(10.0, 1000.0), 2)  
        sample_products.append((product_id, name, description, price, category))

    # Generate Users
    for _ in range(20):  
        user_id = uuid.uuid4()
        name = fake.name()
        email = fake.email()
        last_name = name.split()[-1]
        sample_users.append((user_id, email, name, last_name))

    # Generate Orders
    for _ in range(5):  
        order_id = uuid.uuid4()
        user_id = random.choice(sample_users)[0]
        order_date = fake.date_time_this_year()
        total = round(random.uniform(50.0, 500.0), 2) 
        sample_orders.append((user_id, order_id, order_date, total))

        # Generate Order Items
        for _ in range(3): 
            product_id, _, _, _, _ = random.choice(sample_products)
            quantity = random.randint(1, 5)
            price_at_purchase = round(random.uniform(10.0, 1000.0), 2) 
            sample_cart_items.append((order_id, product_id, quantity, price_at_purchase))

    # Generate Product Reviews
    for product_id, _, _, _, _ in sample_products:
        for _ in range(3): 
            review_id = uuid.uuid4()
            user_id = random.choice(sample_users)[0]
            rating = random.randint(1, 5)
            review = fake.sentence()
            created_at = fake.date_time_this_year()
            sample_reviews.append((product_id, review_id, user_id, rating, review, created_at))

    # Generate Product Views
    for product_id, _, _, _, _ in sample_products:
        for _ in range(5):
            view_timestamp = fake.date_time_this_year()
            sample_views.append((product_id, view_timestamp))

    # Generate Shipments
    for order_id, _, order_date, _ in sample_orders:
        shipped_at = order_date + timedelta(days=random.randint(1, 7))
        estimated_arrival = shipped_at + timedelta(days=random.randint(3, 7))
        tracking_number = fake.uuid4()
        sample_shipments.append((order_id, 'shipped', shipped_at, estimated_arrival, tracking_number))

    # Generate Cart Items (for user carts)
    for user_id, _, _, _ in sample_users:
        for _ in range(2):  
            product_id, _, _, _, _ = random.choice(sample_products)
            quantity = random.randint(1, 3)
            added_at = fake.date_time_this_year()
            added_at_str = added_at.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string format
            sample_cart_items.append((user_id, product_id, quantity, added_at_str))

    return {
        "users": sample_users,
        "products": sample_products,
        "orders": sample_orders,
        "reviews": sample_reviews,
        "cart_items": sample_cart_items,
        "views": sample_views,
        "shipments": sample_shipments
    }
    
def print_generated_data(sample_data):
    for data_type, records in sample_data.items():
        print(f"--- {data_type.upper()} ---")
        for record in records:
            print(record)
        print("\n") 


# Insert data into Cassandra tables
def insert_data(session, sample_data):
    # Insert Users
    for user_id, email, name, _ in sample_data["users"]:
        session.execute("""
            INSERT INTO users_by_id (user_id, email, name) 
            VALUES (%s, %s, %s)
        """, (user_id, email, name))

        session.execute("""
            INSERT INTO users_by_email (email, user_id, name) 
            VALUES (%s, %s, %s)
        """, (email, user_id, name))

        session.execute("""
            INSERT INTO users_by_last_name (last_name, user_id, email, name) 
            VALUES (%s, %s, %s, %s)
        """, (name.split()[-1], user_id, email, name))

    # Insert Products
    for product_id, name, description, price, category in sample_data["products"]:
        session.execute("""
            INSERT INTO products_by_id (product_id, name, description, price, category) 
            VALUES (%s, %s, %s, %s, %s)
        """, (product_id, name, description, price, category))

        session.execute("""
            INSERT INTO products_by_category (category, product_id, name, price) 
            VALUES (%s, %s, %s, %s)
        """, (category, product_id, name, price))

    # Insert Orders
    for user_id, order_id, order_date, total in sample_data["orders"]:
        session.execute("""
            INSERT INTO orders_by_user_id (user_id, order_id, order_date, total) 
            VALUES (%s, %s, %s, %s)
        """, (user_id, order_id, order_date, total))

    # Insert Order Items
    for order_id, product_id, quantity, price_at_purchase in sample_data["cart_items"]:
        session.execute("""
            INSERT INTO order_items_by_order_id (order_id, product_id, quantity, price_at_purchase) 
            VALUES (%s, %s, %s, %s)
        """, (order_id, product_id, quantity, price_at_purchase))

    # Insert Reviews
    for product_id, review_id, user_id, rating, review, created_at in sample_data["reviews"]:
        session.execute("""
            INSERT INTO reviews_by_product (product_id, review_id, user_id, rating, review, created_at) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (product_id, review_id, user_id, rating, review, created_at))

    # Insert Views
    for product_id, view_timestamp in sample_data["views"]:
        session.execute("""
            INSERT INTO product_views (product_id, view_timestamp) 
            VALUES (%s, %s)
        """, (product_id, view_timestamp))

    # Insert Shipments
    for order_id, status, shipped_at, estimated_arrival, tracking_number in sample_data["shipments"]:
        session.execute("""
            INSERT INTO shipment_by_order (order_id, status, shipped_at, estimated_arrival, tracking_number) 
            VALUES (%s, %s, %s, %s, %s)
        """, (order_id, status, shipped_at, estimated_arrival, tracking_number))

    # Insert Cart Items
    for user_id, product_id, quantity, added_at in sample_data["cart_items"]:
        session.execute("""
            INSERT INTO cart_by_user (user_id, product_id, quantity, added_at) 
            VALUES (%s, %s, %s, %s)
        """, (user_id, product_id, quantity, added_at))

# Generate and Insert Data
sample_data = generate_sample_data()
#print_generated_data(sample_data)
insert_data(session, sample_data)

print("All demo data inserted successfully!")

