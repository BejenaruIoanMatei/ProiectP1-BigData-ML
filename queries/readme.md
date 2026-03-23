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
    }
    PRODUCTS {
        int id PK
        string title
        decimal price
    }
    REVIEWS {
        int id PK
        int product_id FK
        int rating
        string comment
        timestamp date
        string reviewer_name
    }
    CARTS {
        int id PK
        int user_id FK
        decimal total
        decimal discounted_total
    }
    CART_ITEMS {
        int id PK
        int cart_id FK
        int product_id FK
        int quantity
        decimal price
    }

    USERS ||--o{ CARTS : "places"
    PRODUCTS ||--o{ REVIEWS : "receives"
    CARTS ||--|{ CART_ITEMS : "contains"
    PRODUCTS ||--o{ CART_ITEMS : "is_in"
```

Mongo diagram:

```mermaid
erDiagram
    ORDERS {
        int cart_id PK
        float total_price
        float discounted_total
        object customer "Embedded Document (Name, Email)"
        array items "Vector: [ {prod_name, qty, price}, ... ]"
    }
    PRODUCTS_CATALOG {
        int product_id PK
        string title
        float price
        array reviews "Vector: [ {rating, comment, user}, ... ]"
    }

    ORDERS ||--o{ PRODUCTS_CATALOG : "references (denormalized)"
```