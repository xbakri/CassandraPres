import uuid
import random
from decimal import Decimal
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from faker import Faker

# Configuration: adjust counts as needed
NUM_USERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 200
MAX_ITEMS_PER_ORDER = 5
MAX_CART_ITEMS = 5
NUM_REVIEWS = 300
NUM_VIEWS = 500

# Connect to Cassandra cluster
cluster = Cluster(['127.0.0.1'])  
session = cluster.connect('ecommerce')  # keyspace name

# Prepare insert statements
insert_user_by_id = session.prepare(
    "INSERT INTO users_by_id (user_id, email, name) VALUES (?, ?, ?)"
)
insert_user_by_email = session.prepare(
    "INSERT INTO users_by_email (email, user_id, name) VALUES (?, ?, ?)"
)
insert_user_by_last_name = session.prepare(
    "INSERT INTO users_by_last_name (last_name, user_id, email, name) VALUES (?, ?, ?, ?)"
)

insert_product_by_id = session.prepare(
    "INSERT INTO products_by_id (product_id, name, description, price, category) VALUES (?, ?, ?, ?, ?)"
)
insert_product_by_category = session.prepare(
    "INSERT INTO products_by_category (category, product_id, name, price) VALUES (?, ?, ?, ?)"
)

insert_order_by_user = session.prepare(
    "INSERT INTO orders_by_user_id (user_id, order_id, order_date, total) VALUES (?, ?, ?, ?)"
)
insert_order_item = session.prepare(
    "INSERT INTO order_items_by_order_id (order_id, product_id, quantity, price_at_purchase) VALUES (?, ?, ?, ?)"
)

insert_review = session.prepare(
    "INSERT INTO reviews_by_product (product_id, review_id, user_id, rating, review, created_at) VALUES (?, ?, ?, ?, ?, ?)"
)

insert_cart_item = session.prepare(
    "INSERT INTO cart_by_user (user_id, product_id, quantity, added_at) VALUES (?, ?, ?, ?)"
)

insert_view = session.prepare(
    "INSERT INTO product_views (product_id, view_timestamp) VALUES (?, ?)"
)

insert_shipment = session.prepare(
    "INSERT INTO shipment_by_order (order_id, status, shipped_at, estimated_arrival, tracking_number) VALUES (?, ?, ?, ?, ?)"
)

fake = Faker()

# Generate products
products = {}
categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Toys']
for _ in range(NUM_PRODUCTS):
    pid = uuid.uuid4()
    name = fake.word().title()
    desc = fake.sentence(nb_words=12)
    price = Decimal(f"{random.uniform(5, 500):.2f}")
    cat = random.choice(categories)
    products[pid] = {'name': name, 'desc': desc, 'price': price, 'category': cat}
    session.execute(insert_product_by_id, (pid, name, desc, price, cat))
    session.execute(insert_product_by_category, (cat, pid, name, price))

# Generate users
users = {}
for _ in range(NUM_USERS):
    uid = uuid.uuid4()
    first = fake.first_name()
    last = fake.last_name()
    full = f"{first} {last}"
    email = fake.email()
    users[uid] = {'name': full, 'email': email, 'last': last}
    session.execute(insert_user_by_id, (uid, email, full))
    session.execute(insert_user_by_email, (email, uid, full))
    session.execute(insert_user_by_last_name, (last, uid, email, full))

# Generate orders and items
orders = []
for _ in range(NUM_ORDERS):
    order_id = uuid.uuid4()
    user_id = random.choice(list(users.keys()))
    date = fake.date_time_between(start_date='-1y', end_date='now')
    num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
    total = Decimal('0.00')
    for _ in range(num_items):
        pid = random.choice(list(products.keys()))
        qty = random.randint(1, 3)
        price = products[pid]['price']
        total += price * qty
        session.execute(insert_order_item, (order_id, pid, qty, price))
    session.execute(insert_order_by_user, (user_id, order_id, date, total))
    orders.append(order_id)

# Generate cart items
for uid in users:
    for _ in range(random.randint(0, MAX_CART_ITEMS)):
        pid = random.choice(list(products.keys()))
        qty = random.randint(1, 5)
        added = fake.date_time_between(start_date='-30d', end_date='now')
        session.execute(insert_cart_item, (uid, pid, qty, added))

# Generate reviews
for _ in range(NUM_REVIEWS):
    pid = random.choice(list(products.keys()))
    review_id = uuid.uuid4()
    uid = random.choice(list(users.keys()))
    rating = random.randint(1, 5)
    text = fake.paragraph(nb_sentences=3)
    created = fake.date_time_between(start_date='-1y', end_date='now')
    session.execute(insert_review, (pid, review_id, uid, rating, text, created))

# Generate product views
for _ in range(NUM_VIEWS):
    pid = random.choice(list(products.keys()))
    timestamp = fake.date_time_between(start_date='-30d', end_date='now')
    session.execute(insert_view, (pid, timestamp))

# Generate shipment records for orders
statuses = ['PENDING', 'SHIPPED', 'DELIVERED', 'CANCELLED']
for oid in orders:
    status = random.choice(statuses)
    shipped_at = None
    est_arrival = None
    tracking = None
    if status in ('SHIPPED', 'DELIVERED'):
        shipped_at = fake.date_time_between(start_date='-2y', end_date='now')
        est_arrival = shipped_at + timedelta(days=random.randint(2, 10))
        tracking = fake.uuid4()
    session.execute(insert_shipment, (oid, status, shipped_at, est_arrival, str(tracking)))

print("Populated Cassandra keyspace 'ecommerce' with fake data.")
