"""
Database Setup Script for AskQL

Creates a DuckDB database with sample data based on the schema CSV file.
Generates realistic test data for customers, products, and orders tables.
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
import random
import os

def create_tables_from_schema(conn, schema_csv_path):
    """
    Create database tables from schema CSV file

    Args:
        conn: DuckDB connection
        schema_csv_path: Path to the schema CSV file
    """
    print("Reading schema from CSV...")
    schema_df = pd.read_csv(schema_csv_path)

    # Group by table
    for table_name in schema_df['table_name'].unique():
        table_cols = schema_df[schema_df['table_name'] == table_name]

        # Build column definitions
        col_defs = []
        for _, col in table_cols.iterrows():
            col_def = f"{col['column_name']} {col['data_type']}"

            # Add NOT NULL constraint
            if col['nullable'] == 'NO':
                col_def += " NOT NULL"

            # Add PRIMARY KEY constraint
            if col['key'] == 'PRI':
                col_def += " PRIMARY KEY"

            col_defs.append(col_def)

        # Create table
        create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
        print(f"Creating table: {table_name}")
        conn.execute(create_sql)

    print("All tables created successfully!")

def generate_sample_data(conn):
    """
    Generate and insert sample data for all tables

    Args:
        conn: DuckDB connection
    """
    print("\nGenerating sample data...")

    # 1. Generate Customers (50 rows)
    print("  - Generating 50 customers...")
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
              'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']

    customers_data = []
    for i in range(1, 51):
        customers_data.append({
            'customer_id': i,
            'name': f'Customer {i}',
            'email': f'customer{i}@example.com',
            'age': random.randint(18, 75),
            'city': random.choice(cities),
            'signup_date': (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')
        })

    customers_df = pd.DataFrame(customers_data)
    conn.execute("INSERT INTO customers SELECT * FROM customers_df")

    # 2. Generate Products (30 rows)
    print("  - Generating 30 products...")
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
    product_names = {
        'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart Watch', 'Camera'],
        'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Sneakers', 'Dress', 'Hat'],
        'Books': ['Fiction Novel', 'Biography', 'Cookbook', 'Self-Help', 'Mystery', 'Sci-Fi'],
        'Home & Garden': ['Coffee Maker', 'Lamp', 'Plant Pot', 'Bedding Set', 'Curtains', 'Rug'],
        'Sports': ['Yoga Mat', 'Dumbbells', 'Basketball', 'Tennis Racket', 'Running Shoes', 'Water Bottle']
    }

    products_data = []
    product_id = 1
    for category in categories:
        for product_name in product_names[category]:
            products_data.append({
                'product_id': product_id,
                'product_name': product_name,
                'category': category,
                'price': round(random.uniform(10, 2000), 2),
                'in_stock': random.choice([True, False])
            })
            product_id += 1

    products_df = pd.DataFrame(products_data)
    conn.execute("INSERT INTO products SELECT * FROM products_df")

    # 3. Generate Orders (200 rows)
    print("  - Generating 200 orders...")
    orders_data = []
    for i in range(1, 201):
        customer_id = random.randint(1, 50)
        product_id = random.randint(1, 30)
        quantity = random.randint(1, 5)

        # Get product price from products table
        price_result = conn.execute(f"SELECT price FROM products WHERE product_id = {product_id}").fetchone()
        price = price_result[0] if price_result else 100.0

        orders_data.append({
            'order_id': i,
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'order_date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            'total_amount': round(price * quantity, 2)
        })

    orders_df = pd.DataFrame(orders_data)
    conn.execute("INSERT INTO orders SELECT * FROM orders_df")

    print("Sample data generated successfully!")

def verify_data(conn):
    """
    Verify that data was inserted correctly

    Args:
        conn: DuckDB connection
    """
    print("\nVerifying data...")

    tables = ['customers', 'products', 'orders']
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  - {table}: {count} rows")

    print("\nDatabase verification complete!")

def main():
    """Main function to set up the database"""
    # Paths
    db_path = "data/askql.duckdb"
    schema_csv = "database_schema.csv"

    # Check if schema CSV exists
    if not os.path.exists(schema_csv):
        print(f"Error: Schema file not found at {schema_csv}")
        return

    # Remove existing database if it exists
    if os.path.exists(db_path):
        print(f"Removing existing database at {db_path}...")
        os.remove(db_path)

    print(f"Creating new database at {db_path}...\n")

    # Create connection
    conn = duckdb.connect(db_path)

    try:
        # Create tables from schema
        create_tables_from_schema(conn, schema_csv)

        # Generate sample data
        generate_sample_data(conn)

        # Verify data
        verify_data(conn)

        print(f"\n✓ Database created successfully at {db_path}")
        print("You can now run askQL.py to query the database!")

    except Exception as e:
        print(f"\n✗ Error creating database: {e}")
        raise

    finally:
        conn.close()

if __name__ == "__main__":
    main()
