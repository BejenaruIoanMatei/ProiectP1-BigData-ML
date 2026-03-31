import os
import random
from fetch_data import fetch_data
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        return conn
    except psycopg2.Error as e:
        print(f"Connection failed: {e}")
        raise

def create_tables(conn):
    print("Creating schema and tables...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.users (
                id INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                password_hash VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS dev.products (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                price DECIMAL(10, 2)
            );

            CREATE TABLE IF NOT EXISTS dev.carts (
                id INT PRIMARY KEY,
                user_id INT REFERENCES dev.users(id),
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS dev.cart_items (
                id SERIAL PRIMARY KEY,
                cart_id INT REFERENCES dev.carts(id),
                product_id INT REFERENCES dev.products(id),
                quantity INT
            );

            CREATE TABLE IF NOT EXISTS dev.reviews (
                id SERIAL PRIMARY KEY,
                product_id INT REFERENCES dev.products(id),
                user_id INT REFERENCES dev.users(id),
                rating INT,
                comment TEXT,
                date TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS dev.addresses (
                id INT PRIMARY KEY,
                user_id INT REFERENCES dev.users(id),
                street VARCHAR(200),
                city VARCHAR(100),
                postal_code VARCHAR(20),
                country VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS dev.orders (
                id INT PRIMARY KEY,
                user_id INT REFERENCES dev.users(id),
                cart_id INT REFERENCES dev.carts(id),
                delivery_address_id INT REFERENCES dev.addresses(id),
                total DECIMAL(10, 2),
                discounted_total DECIMAL(10, 2),
                status VARCHAR(20) CHECK (
                    status IN ('pending','paid','cancelled','shipped','delivered')
                ),
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS dev.order_items (
                id INT PRIMARY KEY,
                order_id INT REFERENCES dev.orders(id),
                product_id INT REFERENCES dev.products(id),
                quantity INT,
                price DECIMAL(10, 2)
            );

            CREATE TABLE IF NOT EXISTS dev.payments (
                id INT PRIMARY KEY,
                order_id INT REFERENCES dev.orders(id),
                amount DECIMAL(10, 2),
                method VARCHAR(20) CHECK (method IN ('card','cash','transfer')),
                transaction_id VARCHAR(100),
                status VARCHAR(20) CHECK (
                    status IN ('pending','completed','failed','refunded')
                ),
                paid_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS dev.courier_companies (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                contact_email VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS dev.couriers (
                id INT PRIMARY KEY,
                company_id INT REFERENCES dev.courier_companies(id),
                full_name VARCHAR(100),
                phone VARCHAR(20),
                vehicle_type VARCHAR(10) CHECK (
                    vehicle_type IN ('bike','car','van','truck')
                )
            );

            CREATE TABLE IF NOT EXISTS dev.warehouses (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                city VARCHAR(100),
                address VARCHAR(200)
            );

            CREATE TABLE IF NOT EXISTS dev.inventory (
                id INT PRIMARY KEY,
                product_id INT REFERENCES dev.products(id),
                warehouse_id INT REFERENCES dev.warehouses(id),
                stock_level INT,
                reserved_quantity INT DEFAULT 0,
                min_stock_level INT DEFAULT 10
            );

            CREATE TABLE IF NOT EXISTS dev.shipments (
                id INT PRIMARY KEY,
                order_id INT REFERENCES dev.orders(id),
                courier_id INT REFERENCES dev.couriers(id),
                origin_warehouse_id INT REFERENCES dev.warehouses(id),
                tracking_number VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS dev.shipment_items (
                id INT PRIMARY KEY,
                shipment_id INT REFERENCES dev.shipments(id),
                order_item_id INT REFERENCES dev.order_items(id),
                quantity INT
            );

            CREATE TABLE IF NOT EXISTS dev.shipment_status (
                id INT PRIMARY KEY,
                shipment_id INT REFERENCES dev.shipments(id),
                status VARCHAR(20) CHECK (
                    status IN ('pending','picked_up','in_transit','delivered','rejected')
                ),
                location VARCHAR(200),
                updated_at TIMESTAMP DEFAULT NOW(),
                observation TEXT
            );
        """)
        conn.commit()
        print("All tables created successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Failed to create tables: {e}")
        raise

def insert_data(conn, data):
    print("Inserting data...")
    cursor = conn.cursor()

    try:
        for u in data['users_data']:
            cursor.execute("""
                INSERT INTO dev.users (id, first_name, last_name, email, password_hash, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
            """, (u['id'], u['firstName'], u['lastName'], u['email'], 'hashed_placeholder'))

        for p in data['products_data']:
            cursor.execute("""
                INSERT INTO dev.products (id, title, price)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (p['id'], p['title'], p['price']))

        user_ids = {u['id'] for u in data['users_data']}
        for cart in data['carts_data']:
            # ghost user
            if cart['userId'] not in user_ids:
                cursor.execute("""
                    INSERT INTO dev.users (id, first_name, last_name, email, password_hash)
                    VALUES (%s, 'Unknown', 'User', %s, 'hashed_placeholder')
                    ON CONFLICT (id) DO NOTHING
                """, (cart['userId'], f"unknown_{cart['userId']}@placeholder.com"))

            cursor.execute("""
                INSERT INTO dev.carts (id, user_id, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
            """, (cart['id'], cart['userId']))

            for item in cart['products']:
                cursor.execute("""
                    INSERT INTO dev.products (id, title, price)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (item['id'], item['title'], item['price']))

                cursor.execute("""
                    INSERT INTO dev.cart_items (cart_id, product_id, quantity)
                    VALUES (%s, %s, %s)
                """, (cart['id'], item['id'], item['quantity']))

        user_email_map = {u['email']: u['id'] for u in data['users_data']}
        all_user_ids   = [u['id'] for u in data['users_data']]

        for p in data['products_data']:
            for r in p.get('reviews', []):
                user_id = user_email_map.get(
                    r.get('reviewerEmail'), random.choice(all_user_ids)
                )
                cursor.execute("""
                    INSERT INTO dev.reviews (product_id, user_id, rating, comment, date)
                    VALUES (%s, %s, %s, %s, %s)
                """, (p['id'], user_id, r['rating'], r['comment'], r['date']))

        for a in data['addresses']:
            cursor.execute("""
                INSERT INTO dev.addresses (id, user_id, street, city, postal_code, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (a['id'], a['user_id'], a['street'], a['city'], a['postal_code'], a['country']))

        for o in data['orders']:
            cursor.execute("""
                INSERT INTO dev.orders
                    (id, user_id, cart_id, delivery_address_id,
                     total, discounted_total, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (o['id'], o['user_id'], o['cart_id'], o['delivery_address_id'],
                  o['total'], o['discounted_total'], o['status'], o['created_at']))

        for oi in data['order_items']:
            cursor.execute("""
                INSERT INTO dev.order_items (id, order_id, product_id, quantity, price)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (oi['id'], oi['order_id'], oi['product_id'], oi['quantity'], oi['price']))

        for pay in data['payments']:
            cursor.execute("""
                INSERT INTO dev.payments
                    (id, order_id, amount, method, transaction_id, status, paid_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (pay['id'], pay['order_id'], pay['amount'], pay['method'],
                  pay['transaction_id'], pay['status'], pay['paid_at']))

        for cc in data['courier_companies']:
            cursor.execute("""
                INSERT INTO dev.courier_companies (id, name, contact_email)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (cc['id'], cc['name'], cc['contact_email']))

        for c in data['couriers']:
            cursor.execute("""
                INSERT INTO dev.couriers (id, company_id, full_name, phone, vehicle_type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (c['id'], c['company_id'], c['full_name'], c['phone'], c['vehicle_type']))

        for wh in data['warehouses']:
            cursor.execute("""
                INSERT INTO dev.warehouses (id, name, city, address)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (wh['id'], wh['name'], wh['city'], wh['address']))

        for inv in data['inventory']:
            cursor.execute("""
                INSERT INTO dev.inventory
                    (id, product_id, warehouse_id, stock_level,
                     reserved_quantity, min_stock_level)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (inv['id'], inv['product_id'], inv['warehouse_id'],
                  inv['stock_level'], inv['reserved_quantity'], inv['min_stock_level']))

        for s in data['shipments']:
            cursor.execute("""
                INSERT INTO dev.shipments
                    (id, order_id, courier_id, origin_warehouse_id,
                     tracking_number, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (s['id'], s['order_id'], s['courier_id'], s['origin_warehouse_id'],
                  s['tracking_number'], s['created_at']))

        for si in data['shipment_items']:
            cursor.execute("""
                INSERT INTO dev.shipment_items
                    (id, shipment_id, order_item_id, quantity)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (si['id'], si['shipment_id'], si['order_item_id'], si['quantity']))

        for ss in data['shipment_statuses']:
            cursor.execute("""
                INSERT INTO dev.shipment_status
                    (id, shipment_id, status, location, updated_at, observation)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (ss['id'], ss['shipment_id'], ss['status'],
                  ss['location'], ss['updated_at'], ss['observation']))

        conn.commit()
        print("All data inserted successfully.")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Insert failed: {e}")
        raise

def main():
    conn = None
    try:
        conn = connect_to_db()
        create_tables(conn)
        data = fetch_data()
        insert_data(conn, data)
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()