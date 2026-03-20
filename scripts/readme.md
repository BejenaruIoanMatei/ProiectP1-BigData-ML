# PostgreSQL to MongoDB migration:

### Tables:

-   **(products + reviews)** in SQL -> **products_catalog** in Mongo -> Array: reviews: [ {rating, comment, user}, ... ]

-   **(carts + cart_items + users)** in SQL -> **orders** in Mongo -> Array: items: [ {product_name, qty, price}, ... ]

### Mongo Express

Go to:

```
http://localhost:8081/db/big_data_db/
```

-   To see **orders** and **products_catalog**