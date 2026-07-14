CREATE TABLE customers (
    customer_id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    age INTEGER,
    city VARCHAR,
    signup_date DATE
);

CREATE TABLE products (
    product_id INTEGER NOT NULL PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    price DECIMAL(10,2),
    in_stock BOOLEAN
);

CREATE TABLE orders (
    order_id INTEGER NOT NULL PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    order_date DATE,
    total_amount DECIMAL(10,2)
);