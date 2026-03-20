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

print(connect_to_db())