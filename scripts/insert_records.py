import os
from fetch_data import fetch_data
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def connect_to_db():
    print("Connecting to the postgresql database...")
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        return conn
    except psycopg2.Error as e:
        print(f"Postgres database connection failed: {e}")
        raise

def create_table(conn):
    print("Creating tables if not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            
            CREATE TABLE IF NOT EXISTS dev.users (
                id INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(50)
            );
            
            CREATE TABLE IF NOT EXISTS dev.products (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                price DECIMAL(10, 2)
            );
            
            CREATE TABLE IF NOT EXISTS dev.carts (
                id INT PRIMARY KEY,
                user_id INT REFERENCES dev.users(id),
                total DECIMAL(10, 2),
                discounted_total DECIMAL(10, 2)
            );
            
            CREATE TABLE IF NOT EXISTS dev.cart_items (
                id SERIAL PRIMARY KEY,
                cart_id INT REFERENCES dev.carts(id),
                product_id INT REFERENCES dev.products(id),
                quantity INT,
                price DECIMAL(10, 2)
            );
            
            CREATE TABLE IF NOT EXISTS dev.reviews (
                id SERIAL PRIMARY KEY,
                product_id INT REFERENCES dev.products(id),
                rating INT,
                comment TEXT,
                date TIMESTAMP,
                reviewer_name VARCHAR(100)
            );
        """)
        conn.commit()
        print("Tables were created successfully in dev schema")
    except psycopg2.Error as e:
        print(f"Failed to create tables in postgres: {e}")
        raise
    
def insert_data(conn, data):
    print("Inserting data into db...")
    try:
        cursor = conn.cursor()
        users_data = data['users_data']
        products_data = data['products_data']
        carts_data = data['carts_data']
        
        for u in users_data:
            cursor.execute("""
                INSERT INTO dev.users (id, first_name, last_name, email)
                VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING
            """,
                (u['id'], u['firstName'], u['lastName'], u['email'])    
            )
        
        for p in products_data:
            cursor.execute("""
                INSERT INTO dev.products (id, title, price)
                VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING            
            """,
                (p['id'], p['title'], p['price'])
            )
            
            if 'reviews' in p:
                for r in p['reviews']:
                    cursor.execute("""
                        INSERT INTO dev.reviews (product_id, rating, comment, date, reviewer_name) 
                        VALUES (%s, %s, %s, %s, %s)
                    """,
                        (p['id'], r['rating'], r['comment'], r['date'], r['reviewerName'])
                    )
        
        for cart in carts_data:
            ## handling incomplete dimension -> ne facem noi un user daca nu exista ca sa nu crape
            cursor.execute("""
                INSERT INTO dev.users (id, first_name, last_name, email)
                VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING               
            """,
                (cart['userId'], 'Unknown', 'User', f"unknown_{cart['userId']}@gmail.com")
            )
            
            cursor.execute("""
                INSERT INTO dev.carts (id, user_id, total, discounted_total)
                VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING            
            """,
                (cart['id'], cart['userId'], cart['total'], cart['discountedTotal'])
            )
            
            for item in cart['products']:
                cursor.execute("""
                    INSERT INTO dev.products (id, title, price)
                    VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING               
                """,
                    (item['id'], item['title'], item['price'])
                )
                
                cursor.execute("""
                    INSERT INTO dev.cart_items (cart_id, product_id, quantity, price)
                    VALUES (%s, %s, %s, %s)               
                """,
                    (cart['id'], item['id'], item['quantity'], item['price'])
                )
        conn.commit()
        print("All data inserted successfully")  
    except psycopg2.Error as e:
        print(f"Failed to insert data into tables: {e}")
        raise
    
def main():
    try:
        conn = connect_to_db()
        create_table(conn)
        data = fetch_data()
        insert_data(conn, data)
    except Exception as e:
        print(f"An error occured during exec: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")
            
if __name__ == "__main__":
    main()