import os
import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_pg_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def get_mongo_client():
    return MongoClient(
        host="mongodb",
        port=27017,
        username=os.getenv("MONGO_USER"),
        password=os.getenv("MONGO_PASSWORD")
    )

def migrate():
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor()
    mongo_client = get_mongo_client()
    db = mongo_client['big_data_db']
    pg_cur.execute("SET search_path TO dev, public;")

    print("Starting migration: PostgreSQL -> MongoDB...")
    print("Migrating products with embedded reviews...")
    pg_cur.execute("SELECT id, title, price FROM products")
    products = pg_cur.fetchall()

    for p_id, title, price in products:
        
        pg_cur.execute("SELECT rating, comment, reviewer_name FROM reviews WHERE product_id = %s", (p_id,))
        reviews_data = pg_cur.fetchall()
        
        reviews_list = [
            {"rating": r[0], "comment": r[1], "user": r[2]} 
            for r in reviews_data
        ]

        db.products_catalog.update_one(
            {"product_id": p_id},
            {"$set": {
                "title": title,
                "price": float(price),
                "reviews": reviews_list
            }},
            upsert=True
        )

    print("Migrating carts with embedded items and user info...")
    pg_cur.execute("""
        SELECT c.id, c.total, c.discounted_total, u.first_name, u.last_name, u.email 
        FROM carts c 
        JOIN users u ON c.user_id = u.id
    """)
    carts = pg_cur.fetchall()

    for c_id, total, discounted_total, f_name, l_name, email in carts:
        pg_cur.execute("""
            SELECT p.title, ci.quantity, ci.price 
            FROM cart_items ci
            JOIN products p ON ci.product_id = p.id
            WHERE ci.cart_id = %s
        """, (c_id,))
        items_data = pg_cur.fetchall()
        
        items_list = [
            {"product_name": i[0], "quantity": i[1], "unit_price": float(i[2])} 
            for i in items_data
        ]

        db.orders.update_one(
            {"cart_id": c_id},
            {"$set": {
                "customer": {"name": f"{f_name} {l_name}", "email": email},
                "total_price": float(total),
                "discounted_total": float(discounted_total),
                "items": items_list
            }},
            upsert=True
        )

    print("Migration finished successfully")
    pg_cur.close()
    pg_conn.close()

if __name__ == "__main__":
    migrate()