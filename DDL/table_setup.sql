CREATE KEYSPACE ecommerce WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
    };

use ecommerce;

// User tables
CREATE TABLE users_by_id (
                             user_id UUID PRIMARY KEY,
                             email TEXT,
                             name TEXT
);

CREATE TABLE users_by_email (
                                email TEXT PRIMARY KEY,
                                user_id UUID,
                                name TEXT
);

CREATE TABLE users_by_last_name (
                                    last_name TEXT,
                                    user_id UUID,
                                    email TEXT,
                                    name TEXT,
                                    PRIMARY KEY (last_name, user_id)
);

// Product tables
CREATE TABLE products_by_id (
                                product_id UUID PRIMARY KEY,
                                name TEXT,
                                description TEXT,
                                price DECIMAL,
                                category TEXT
);

CREATE TABLE products_by_category (
                                      category TEXT,
                                      product_id UUID,
                                      name TEXT,
                                      price DECIMAL,
                                      PRIMARY KEY (category, product_id)
);

// Order tables
CREATE TABLE orders_by_user_id (
                                   user_id UUID,
                                   order_id UUID,
                                   order_date TIMESTAMP,
                                   total DECIMAL,
                                   PRIMARY KEY (user_id, order_id)
);

CREATE TABLE order_items_by_order_id (
                                         order_id UUID,
                                         product_id UUID,
                                         quantity INT,
                                         price_at_purchase DECIMAL,
                                         PRIMARY KEY (order_id, product_id)
);

// Product reviews
CREATE TABLE reviews_by_product (
                                    product_id UUID,
                                    review_id UUID,
                                    user_id UUID,
                                    rating INT,
                                    review TEXT,
                                    created_at TIMESTAMP,
                                    PRIMARY KEY (product_id, review_id)
) WITH CLUSTERING ORDER BY (review_id DESC);

// Shopping cart by user
CREATE TABLE cart_by_user (
                              user_id UUID,
                              product_id UUID,
                              quantity INT,
                              added_at TIMESTAMP,
                              PRIMARY KEY (user_id, product_id)
);

// Analytics
CREATE TABLE product_views (
   product_id UUID,
   view_timestamp TIMESTAMP,
   PRIMARY KEY (product_id, view_timestamp)
) WITH CLUSTERING ORDER BY (view_timestamp DESC);

// Shipping
CREATE TABLE shipment_by_order (
       order_id UUID,
       status TEXT,
       shipped_at TIMESTAMP,
       estimated_arrival TIMESTAMP,
       tracking_number TEXT,
       PRIMARY KEY (order_id)
);




