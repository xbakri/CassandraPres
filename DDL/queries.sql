#Show all orders by a user
SELECT * FROM orders_by_user_id WHERE user_id = f6e254bf-811f-4bfa-87bd-a5abd367599c;

#Get products in one specific order
SELECT * FROM order_items_by_order_id WHERE order_id = 68fb4808-9dd6-4f1e-b711-6b74142b2ca4;

#All reviews for a product (most recent first)
SELECT * FROM reviews_by_product WHERE product_id = 6875f44c-3e87-471b-9ae4-4dfa3957b3e8;

#All products in a category
SELECT * FROM products_by_category WHERE category = 'Electronics';

#Track shipping for an order
SELECT * FROM shipment_by_order WHERE order_id = 68fb4808-9dd6-4f1e-b711-6b74142b2ca4;


#Find the top 5 most recent reviews for a product
SELECT review_id, rating, review, created_at
FROM reviews_by_product
WHERE product_id = b7c0e41a-dee3-4720-bbe5-29b2b71c4c0a
LIMIT 5
ALLOW FILTERING;


#See what's in a user's cart
SELECT product_id, quantity, added_at
FROM cart_by_user
WHERE user_id = f6e254bf-811f-4bfa-87bd-a5abd367599c;

#Get the total number of orders and amount spent by a user
SELECT order_id, total
FROM orders_by_user_id
WHERE user_id = f6e254bf-811f-4bfa-87bd-a5abd367599c;

#Find all users with a specific last name
SELECT user_id, email, name
FROM users_by_last_name
WHERE last_name = 'Weaver';

#Fetch all items from a specific order (with price at time of purchase
SELECT product_id, quantity, price_at_purchase
FROM order_items_by_order_id
WHERE order_id = 68fb4808-9dd6-4f1e-b711-6b74142b2ca4;

#Show the last 10 views for a product
SELECT view_timestamp
FROM product_views
WHERE product_id = 6875f44c-3e87-471b-9ae4-4dfa3957b3e8
LIMIT 10;

#Track a shipment for an order
SELECT status, shipped_at, estimated_arrival, tracking_number
FROM shipment_by_order
WHERE order_id = 68fb4808-9dd6-4f1e-b711-6b74142b2ca4;

# Show all products in "electronics" sorted by product_id
SELECT product_id, name, price
FROM products_by_category
WHERE category = 'Electronics';

