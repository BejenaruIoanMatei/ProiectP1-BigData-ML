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
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(50)
            );
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                price DECIMAL(10, 2)
            );
            CREATE TABLE IF NOT EXISTS carts (
                id INT PRIMARY KEY,
                user_id INT REFERENCES users(id),
                total DECIMAL(10, 2),
                discounted_total DECIMAL(10, 2)
            );
            CREATE TABLE IF NOT EXISTS cart_items (
                id SERIAL PRIMARY KEY,
                cart_id INT REFERENCES carts(id),
                product_id INT REFERENCES products(id),
                quantity INT,
                price DECIMAL(10, 2)
            );  
        """)
    except psycopg2.Error as e:
        print(f"Failed to create tables in postgres: {e}")
        raise