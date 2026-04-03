### About

Here you create new directory where you upload the queries:

Example:

```
mkdir iulian
cd iulian
nano script1
```

Install Mermaid extension to see the postgres database ERD diagram:

```mermaid
erDiagram
    USERS {
        int id PK
        string first_name
        string last_name
        string email
        string password_hash
        timestamp created_at
    }
    PRODUCTS {
        int id PK
        string title
        decimal price
    }
    REVIEWS {
        int id PK
        int product_id FK
        int user_id FK
        int rating
        string comment
        timestamp date
    }

    CARTS {
        int id PK
        int user_id FK
        timestamp updated_at
    }
    CART_ITEMS {
        int id PK
        int cart_id FK
        int product_id FK
        int quantity
    }

    ADDRESSES {
        int id PK
        int user_id FK
        string street
        string city
        string postal_code
        string country
    }

    ORDERS {
        int id PK
        int user_id FK
        int cart_id FK
        int delivery_address_id FK
        decimal total
        decimal discounted_total
        enum status "pending, paid, cancelled, shipped, delivered"
        timestamp created_at
    }
    ORDER_ITEMS {
        int id PK
        int order_id FK
        int product_id FK
        int quantity
        decimal price "Prețul la momentul achiziției"
    }

    PAYMENTS {
        int id PK
        int order_id FK
        decimal amount
        string method "card, cash, transfer"
        string transaction_id
        enum status "pending, completed, failed, refunded"
        timestamp paid_at
    }

    COURIER_COMPANIES {
        int id PK
        string name
        string contact_email
    }
    COURIERS {
        int id PK
        int company_id FK
        string full_name
        string phone
        enum vehicle_type "bike, car, van, truck"
    }

    SHIPMENTS {
        int id PK
        int order_id FK
        int courier_id FK
        int origin_warehouse_id FK
        string tracking_number
        timestamp created_at
    }

    SHIPMENT_ITEMS {
        int id PK
        int shipment_id FK
        int order_item_id FK
        int quantity
    }

    SHIPMENT_STATUS {
        int id PK
        int shipment_id FK
        enum status "pending, picked_up, in_transit, delivered, rejected"
        string location "Lat/Long sau Oraș"
        timestamp updated_at
        string observation "Ex: Clientul nu a raspuns"
    }

    WAREHOUSES {
        int id PK
        string name
        string city
        string address
    }

    INVENTORY {
        int id PK
        int product_id FK
        int warehouse_id FK
        int stock_level "Stoc fizic total"
        int reserved_quantity "Stoc blocat pentru comenzi neexpediate"
        int min_stock_level "Prag de alertă"
    }

    USERS ||--o{ CARTS : "has"
    USERS ||--o{ ORDERS : "places"
    USERS ||--o{ REVIEWS : "writes"
    USERS ||--o{ ADDRESSES : "has"

    CARTS ||--|{ CART_ITEMS : "contains"
    PRODUCTS ||--o{ CART_ITEMS : "is_in"

    CARTS ||--o| ORDERS : "converts_to"
    ADDRESSES ||--o{ ORDERS : "ships_to"
    ORDERS ||--|{ ORDER_ITEMS : "contains"
    PRODUCTS ||--o{ ORDER_ITEMS : "is_in"
    PRODUCTS ||--o{ REVIEWS : "receives"

    ORDERS ||--o{ PAYMENTS : "paid_via"

    COURIER_COMPANIES ||--o{ COURIERS : "employs"
    COURIERS ||--o{ SHIPMENTS : "delivers"
    ORDERS ||--o{ SHIPMENTS : "generates"
    WAREHOUSES ||--o{ SHIPMENTS : "dispatches_from"
    SHIPMENTS ||--|{ SHIPMENT_ITEMS : "contains"
    ORDER_ITEMS ||--o{ SHIPMENT_ITEMS : "fulfilled_by"
    SHIPMENTS ||--|{ SHIPMENT_STATUS : "tracks"
    
    WAREHOUSES ||--o{ INVENTORY : "stores"
    PRODUCTS ||--o{ INVENTORY : "is_stocked_as"
```

Mongo diagram:

```mermaid
erDiagram

    USERS {
        objectId _id PK
        string first_name
        string last_name
        string email
        string password_hash
        timestamp created_at
        array addresses "Vector: [{street, city, postal_code, country, is_default}]"
    }

    PRODUCTS {
        objectId _id PK
        string title
        decimal price
        array reviews "Vector: [{user_id, rating, comment, date}]"
        array inventory "Vector: [{warehouse_id, stock_level, reserved_quantity, min_stock_level}]"
    }

    CARTS {
        objectId _id PK
        objectId user_id FK
        timestamp updated_at
        array items "Vector: [{product_id, title, price, quantity}]"
    }

    ORDERS {
        objectId _id PK
        objectId user_id FK
        objectId cart_id FK
        object delivery_address "Snapshot: {street, city, postal_code, country}"
        decimal total
        decimal discounted_total
        enum status "pending, paid, cancelled, shipped, delivered"
        timestamp created_at
        array items "Vector: [{product_id, title, quantity, price}]"
        object payment "Embedded: {amount, method, transaction_id, status, paid_at}"
        object shipment "Embedded: {courier_id, warehouse_id, tracking_number, created_at, items[], status_history[]}"
    }

    COURIERS {
        objectId _id PK
        string full_name
        string phone
        enum vehicle_type "bike, car, van, truck"
        object company "Denormalizat: {name, contact_email}"
    }

    WAREHOUSES {
        objectId _id PK
        string name
        string city
        string address
    }

    USERS ||--o{ CARTS : "has"
    USERS ||--o{ ORDERS : "places"
    PRODUCTS ||--o{ CARTS : "referenced in items[]"
    PRODUCTS ||--o{ ORDERS : "referenced in items[]"
    COURIERS ||--o{ ORDERS : "referenced in shipment"
    WAREHOUSES ||--o{ ORDERS : "referenced in shipment"
    WAREHOUSES ||--o{ PRODUCTS : "referenced in inventory[]"
```