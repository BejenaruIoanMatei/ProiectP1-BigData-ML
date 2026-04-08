import os
import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_pg_connection():
    print("Connecting to postgres...")
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
        print(f"Connection failed: {e}")
        raise
        
def get_mongo_client():
    return MongoClient(
        host=os.getenv("MONGO_HOST", "mongodb"),
        port=int(os.getenv("MONGO_PORT", 27017)),
        username=os.getenv("MONGO_USER"),
        password=os.getenv("MONGO_PASSWORD")
    )

def fetchall_as_dicts(cursor, query, params=None):
    """
        Runs query and returns dict list
    """
    cursor.execute(query, params or ())
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def migrate_warehouses(cur, db):
    print("  Migrating warehouses...")
    rows = fetchall_as_dicts(cur, "SELECT id, name, city, address FROM warehouses")
    id_map = {}

    for row in rows:
        doc = {
            "pg_id": row["id"],
            "name": row["name"],
            "city": row["city"],
            "address": row["address"],
        }
        result = db.warehouses.update_one(
            {"pg_id": row["id"]},
            {"$set": doc},
            upsert=True
        )
        oid = result.upserted_id or db.warehouses.find_one({"pg_id": row["id"]})["_id"]
        id_map[row["id"]] = oid

    print(f"{len(rows)} warehouses migrated.")
    return id_map

def migrate_couriers(cur, db):
    print("Migrating couriers (with embedded company)...")
    rows = fetchall_as_dicts(cur, """
        SELECT cu.id, cu.full_name, cu.phone, cu.vehicle_type,
               cc.name AS company_name, cc.contact_email AS company_email
        FROM couriers cu
        JOIN courier_companies cc ON cu.company_id = cc.id
    """)
    id_map = {}

    for row in rows:
        doc = {
            "pg_id": row["id"],
            "full_name": row["full_name"],
            "phone": row["phone"],
            "vehicle_type": row["vehicle_type"],
            "company": {
                "name": row["company_name"],
                "contact_email": row["company_email"],
            },
        }
        result = db.couriers.update_one(
            {"pg_id": row["id"]},
            {"$set": doc},
            upsert=True
        )
        oid = result.upserted_id or db.couriers.find_one({"pg_id": row["id"]})["_id"]
        id_map[row["id"]] = oid

    print(f"{len(rows)} couriers migrated.")
    return id_map

def migrate_products(cur, db, warehouse_id_map):
    print("Migrating products (with embedded reviews + inventory)...")
    products = fetchall_as_dicts(cur, "SELECT id, title, price FROM products")
    id_map = {}

    for p in products:
        reviews = fetchall_as_dicts(cur, """
            SELECT r.rating, r.comment, r.date,
                   u.id AS user_pg_id
            FROM reviews r
            LEFT JOIN users u ON r.user_id = u.id
            WHERE r.product_id = %s
        """, (p["id"],))

        reviews_list = [
            {
                "user_pg_id": r["user_pg_id"],
                "rating": r["rating"],
                "comment": r["comment"],
                "date": r["date"],
            }
            for r in reviews
        ]
        inventory = fetchall_as_dicts(cur, """
            SELECT warehouse_id, stock_level, reserved_quantity, min_stock_level
            FROM inventory
            WHERE product_id = %s
        """, (p["id"],))

        inventory_list = [
            {
                "warehouse_id": warehouse_id_map.get(i["warehouse_id"]),
                "stock_level": i["stock_level"],
                "reserved_quantity": i["reserved_quantity"],
                "min_stock_level": i["min_stock_level"],
            }
            for i in inventory
        ]

        doc = {
            "pg_id": p["id"],
            "title": p["title"],
            "price": float(p["price"]),
            "reviews": reviews_list,
            "inventory": inventory_list,
        }

        result = db.products.update_one(
            {"pg_id": p["id"]},
            {"$set": doc},
            upsert=True
        )
        oid = result.upserted_id or db.products.find_one({"pg_id": p["id"]})["_id"]
        id_map[p["id"]] = oid

    print(f"{len(products)} products migrated.")
    return id_map

def migrate_users(cur, db):
    print("Migrating users (with embedded addresses)...")
    users = fetchall_as_dicts(cur, """
        SELECT id, first_name, last_name, email, password_hash, created_at
        FROM users
    """)
    id_map = {}

    for u in users:
        addresses = fetchall_as_dicts(cur, """
            SELECT street, city, postal_code, country
            FROM addresses
            WHERE user_id = %s
        """, (u["id"],))

        doc = {
            "pg_id": u["id"],
            "first_name": u["first_name"],
            "last_name": u["last_name"],
            "email": u["email"],
            "password_hash": u["password_hash"],
            "created_at": u["created_at"],
            "addresses": addresses,
        }

        result = db.users.update_one(
            {"pg_id": u["id"]},
            {"$set": doc},
            upsert=True
        )
        oid = result.upserted_id or db.users.find_one({"pg_id": u["id"]})["_id"]
        id_map[u["id"]] = oid

    print(f"{len(users)} users migrated.")
    return id_map

def migrate_carts(cur, db, user_id_map, product_id_map):
    print("Migrating carts (with embedded items)...")
    carts = fetchall_as_dicts(cur, "SELECT id, user_id, updated_at FROM carts")
    id_map = {}

    for c in carts:
        items = fetchall_as_dicts(cur, """
            SELECT ci.product_id, ci.quantity, p.title, p.price
            FROM cart_items ci
            JOIN products p ON ci.product_id = p.id
            WHERE ci.cart_id = %s
        """, (c["id"],))

        items_list = [
            {
                "product_id": product_id_map.get(i["product_id"]),
                "title": i["title"],
                "price": float(i["price"]),
                "quantity": i["quantity"],
            }
            for i in items
        ]

        doc = {
            "pg_id": c["id"],
            "user_id": user_id_map.get(c["user_id"]),
            "updated_at": c["updated_at"],
            "items": items_list,
        }

        result = db.carts.update_one(
            {"pg_id": c["id"]},
            {"$set": doc},
            upsert=True
        )
        oid = result.upserted_id or db.carts.find_one({"pg_id": c["id"]})["_id"]
        id_map[c["id"]] = oid

    print(f"{len(carts)} carts migrated.")
    return id_map

def migrate_orders(cur, db, user_id_map, cart_id_map, product_id_map,
                   courier_id_map, warehouse_id_map):
    print("Migrating orders (with embedded items, payment, shipment)...")
    orders = fetchall_as_dicts(cur, """
        SELECT o.id, o.user_id, o.cart_id, o.total, o.discounted_total,
               o.status, o.created_at,
               a.street, a.city, a.postal_code, a.country
        FROM orders o
        LEFT JOIN addresses a ON o.delivery_address_id = a.id
    """)

    for o in orders:
        items = fetchall_as_dicts(cur, """
            SELECT oi.id AS oi_pg_id, oi.product_id, oi.quantity, oi.price, p.title
            FROM order_items oi
            JOIN products p ON oi.product_id = p.id
            WHERE oi.order_id = %s
        """, (o["id"],))

        oi_index_map = {item["oi_pg_id"]: idx for idx, item in enumerate(items)}

        items_list = [
            {
                "product_id": product_id_map.get(i["product_id"]),
                "title": i["title"],
                "quantity": i["quantity"],
                "price": float(i["price"]),
            }
            for i in items
        ]

        payments = fetchall_as_dicts(cur, """
            SELECT amount, method, transaction_id, status, paid_at
            FROM payments
            WHERE order_id = %s
            LIMIT 1
        """, (o["id"],))

        payment_doc = None
        if payments:
            pay = payments[0]
            payment_doc = {
                "amount": float(pay["amount"]),
                "method": pay["method"],
                "transaction_id": pay["transaction_id"],
                "status": pay["status"],
                "paid_at": pay["paid_at"],
            }

        shipments = fetchall_as_dicts(cur, """
            SELECT id, courier_id, origin_warehouse_id, tracking_number, created_at
            FROM shipments
            WHERE order_id = %s
            LIMIT 1
        """, (o["id"],))

        shipment_doc = None
        if shipments:
            s = shipments[0]

            ship_items = fetchall_as_dicts(cur, """
                SELECT order_item_id, quantity
                FROM shipment_items
                WHERE shipment_id = %s
            """, (s["id"],))

            ship_items_list = [
                {
                    "order_item_index": oi_index_map.get(si["order_item_id"]),
                    "quantity": si["quantity"],
                }
                for si in ship_items
            ]

            status_history = fetchall_as_dicts(cur, """
                SELECT status, location, updated_at, observation
                FROM shipment_status
                WHERE shipment_id = %s
                ORDER BY updated_at ASC
            """, (s["id"],))

            shipment_doc = {
                "courier_id": courier_id_map.get(s["courier_id"]),
                "warehouse_id": warehouse_id_map.get(s["origin_warehouse_id"]),
                "tracking_number": s["tracking_number"],
                "created_at": s["created_at"],
                "items": ship_items_list,
                "status_history": [
                    {
                        "status": sh["status"],
                        "location": sh["location"],
                        "updated_at": sh["updated_at"],
                        "observation": sh["observation"],
                    }
                    for sh in status_history
                ],
            }

        doc = {
            "pg_id": o["id"],
            "user_id": user_id_map.get(o["user_id"]),
            "cart_id": cart_id_map.get(o["cart_id"]),
            "delivery_address": {
                "street": o["street"],
                "city": o["city"],
                "postal_code": o["postal_code"],
                "country": o["country"],
            },
            "total": float(o["total"]),
            "discounted_total":  float(o["discounted_total"]),
            "status": o["status"],
            "created_at": o["created_at"],
            "items": items_list,
            "payment": payment_doc,
            "shipment": shipment_doc,
        }

        db.orders.update_one(
            {"pg_id": o["id"]},
            {"$set": doc},
            upsert=True
        )

    print(f"{len(orders)} orders migrated.")

def migrate():
    pg_conn = get_pg_connection()
    cur = pg_conn.cursor()
    mongo_client = get_mongo_client()
    db = mongo_client["big_data_db"]

    cur.execute("SET search_path TO dev, public;")

    print("Starting migration: PostgreSQL → MongoDB")
    print("=" * 50)

    warehouse_id_map = migrate_warehouses(cur, db)
    courier_id_map = migrate_couriers(cur, db)
    product_id_map = migrate_products(cur, db, warehouse_id_map)
    user_id_map = migrate_users(cur, db)
    cart_id_map = migrate_carts(cur, db, user_id_map, product_id_map)
    migrate_orders(
        cur, db,
        user_id_map, cart_id_map, product_id_map,
        courier_id_map, warehouse_id_map
    )

    print("=" * 50)
    print("Migration finished successfully.")
    print("Collections created: warehouses, couriers, products, users, carts, orders")

    cur.close()
    pg_conn.close()
    mongo_client.close()


if __name__ == "__main__":
    migrate()
