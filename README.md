# Sisteme Big Data pentru Machine Learning
## ETL Pipeline: PostgreSQL → MongoDB

Proiect end-to-end de inginerie a datelor care implementează un pipeline ETL complet, de la extragerea datelor dintr-un API extern, modelarea într-o schemă relațională PostgreSQL, până la migrarea și transformarea lor într-o arhitectură NoSQL MongoDB.

---

## Arhitectura sistemului

```
[API Extern + Mock Data] ──► [PostgreSQL - Staging] ──► [Python Migration Script] ──► [MongoDB]
                      (15 tabele)                                          (6 colecții)
```

1. **Extraction** — scripturi Python extrag date reale din API-ul public `dummyjson.com` (users, products, carts, reviews) și generează date mock realiste pentru restul domeniului (adrese, comenzi, plăți, curierat, depozite, stocuri, livrări).
2. **Staging (PostgreSQL)** — datele sunt persistate într-o schemă relațională normalizată cu 16 tabele, asigurând integritate referențială și auditabilitate.
3. **Transformation & Load (MongoDB)** — un script de migrare Python citește din PostgreSQL, denormalizează datele prin document embedding și le încarcă în 6 colecții MongoDB.

---

## Modelarea datelor

### PostgreSQL — schema relațională (15 tabele)

Schema acoperă trei domenii funcționale:

**E-commerce:** `users`, `addresses`, `products`, `reviews`, `carts`, `cart_items`, `orders`, `order_items`, `payments`

**Logistică:** `courier_companies`, `couriers`, `shipments`, `shipment_items`, `shipment_status`

**Warehouse & Inventory:** `warehouses`, `inventory`

### MongoDB — schema documentară (6 colecții)

Modelarea MongoDB prioritizează **data locality** — datele citite împreună sunt stocate împreună, eliminând nevoia de JOIN-uri.

| Colecție | Tabele absorbite | Arrays embedded |
|---|---|---|
| `users` | USERS + ADDRESSES | `addresses[]` |
| `products` | PRODUCTS + REVIEWS + INVENTORY | `reviews[]`, `inventory[]` |
| `carts` | CARTS + CART_ITEMS | `items[]` |
| `orders` | ORDERS + ORDER_ITEMS + PAYMENTS + SHIPMENTS + SHIPMENT_ITEMS + SHIPMENT_STATUS | `items[]`, `shipment.items[]`, `shipment.status_history[]` |
| `couriers` | COURIERS + COURIER_COMPANIES | — (denormalizat) |
| `warehouses` | WAREHOUSES | — |

---

## Tech Stack

| Categorie | Tehnologii |
|---|---|
| Baze de date | PostgreSQL, MongoDB |
| Scripting | Python 3, psycopg2, pymongo, requests |
| Management UI | Mongo Express |
| Infrastructură | Docker, Docker Compose |

---

## Structura proiectului

```
.
├── notebooks
│   └── queries_testing.sql
├── postgres
│   └── data  [error opening dir]
├── postgres_conn.sh
├── queries
│   ├── ERD_postgres.png
│   ├── MongoDiagram.png
│   ├── iulian
│   │   ├── mongo_queries.js
│   │   └── sql_queries.sql
│   ├── matei
│   │   ├── mongo_queries.js
│   │   └── sql_queries.sql
│   ├── pgAdminERD.png
│   ├── readme.md
│   └── teofil
│       ├── mongo_queries.js
│       └── sql_queries.sql
├── requirements.txt
├── scripts
│   ├── __pycache__
│   │   ├── fetch_data.cpython-311.pyc
│   │   └── fetch_data.cpython-312.pyc
│   ├── drop_database_mongo.py
│   ├── fetch_data.py
│   ├── insert_records.py
│   ├── migrate_to_mongo.py
│   ├── readme.md
│   └── test_connection.py
├── test_app.sh
├── test_db.sh
```

---

## How to run 

```bash
cp .env.example .env # si se completeaza in .env

# Pornire servicii
docker-compose up -d
docker ps (pentru a verifica daca apar containerele)

# Se testeaza conexiunile cu:
./test_app.sh
./test_db.sh
## chmod +x test_app.sh
### pentru a da drept de execute
./postgress_conn.sh
# Te conecteaza la postgres, unde poti vedea tabelele, schema, etc
## \dn, \dt dev.*, etc
### ca sa iesi \q

Daca totul e ok pana aici, atunci se ruleaza scripturile in urmatoarea ordine:
docker compose exec python_app scripts/fetch_app.py
# Sau direct din containerul de python
docker compose exec -it python_app sh
ls -l #pentru a vedea structura dir
python scripts/fetch_app.py
python scripts/insert_records.py
python scripts/migrate_to_mongo.py
```

Servicii disponibile după pornire:
- **PostgreSQL** — `localhost:5432`
- **MongoDB** — `localhost:27017`
- **Mongo Express** — `http://localhost:8081`

OBS:

-    La interogarile din SQL au mai fost adaugate observatii in tabele pentru a verifica daca interogarile functioneaza corect
