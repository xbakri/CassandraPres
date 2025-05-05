#Show all orders by a user
SELECT * FROM orders_by_user_id WHERE user_id = [some UUID];

#Get products in one specific order
SELECT * FROM order_items_by_order_id WHERE order_id = [ordlaunch cqler UUID];

#All reviews for a product (most recent first)
SELECT * FROM reviews_by_product WHERE product_id = [UUID];

#All products in a category
SELECT * FROM products_by_category WHERE category = 'Electronics';

#Track shipping for an order
SELECT * FROM shipment_by_order WHERE order_id = [order UUID];