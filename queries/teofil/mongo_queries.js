// useri peste medie
db.orders.aggregate([
  { "$group": { 
      "_id": "$user_id", 
      "total_spent": { "$sum": "$total" } 
  }},
  { "$group": {
      "_id": null,
      "avg_spent": { "$avg": "$total_spent" },
      "all_users": { "$push": { "user_id": "$_id", "total_spent": "$total_spent" } }
  }},
  { "$unwind": "$all_users" },
  { "$match": { 
      "$expr": { "$gt": [ "$all_users.total_spent", "$avg_spent" ] } 
  }},
  { "$lookup": { 
      "from": "users", 
      "localField": "all_users.user_id", 
      "foreignField": "_id", 
      "as": "user" 
  }},
  { "$unwind": "$user" },
  { "$project": { 
      "_id": 0, 
      "first_name": "$user.first_name", 
      "last_name": "$user.last_name", 
      "total_spent": "$all_users.total_spent" 
  }},
  { "$sort": { "total_spent": -1 } }
])


// depozitele in care s-a platit cel mai mult cu cash 
db.orders.aggregate([
  { "$match": { 
      "payment.method": "cash", 
      "payment.status": "completed" 
  }},
  { "$group": { 
      "_id": "$shipment.warehouse_id", 
      "total_cash_transactions": { "$sum": 1 }, 
      "total_revenue_from_cash": { "$sum": "$payment.amount" } 
  }},
  { "$group": {
      "_id": null,
      "max_revenue": { "$max": "$total_revenue_from_cash" },
      "all_warehouses": { 
          "$push": { 
              "warehouse_id": "$_id", 
              "total_cash_transactions": "$total_cash_transactions",
              "total_revenue_from_cash": "$total_revenue_from_cash" 
          } 
      }
  }},
  { "$unwind": "$all_warehouses" },
  { "$match": { 
      "$expr": { "$eq": [ "$all_warehouses.total_revenue_from_cash", "$max_revenue" ] } 
  }},
  { "$lookup": { 
      "from": "warehouses", 
      "localField": "all_warehouses.warehouse_id", 
      "foreignField": "_id", 
      "as": "warehouse_data" 
  }},
  { "$unwind": "$warehouse_data" },
  { "$project": { 
      "_id": 0, 
      "warehouse_name": "$warehouse_data.name", 
      "city": "$warehouse_data.city", 
      "total_cash_transactions": "$all_warehouses.total_cash_transactions",
      "total_revenue_from_cash": "$all_warehouses.total_revenue_from_cash" 
  }}
])

/// depozitele cu cele mai mari vanzari
db.orders.aggregate([
  { "$match": { 
      "status": { "$in": ["paid", "shipped", "delivered"] } 
  }},
  { "$group": { 
      "_id": "$shipment.warehouse_id", 
      "total_successful_orders": { "$sum": 1 }, 
      "total_sales_volume": { "$sum": "$discounted_total" } 
  }},
  { "$group": {
      "_id": null,
      "max_sales": { "$max": "$total_sales_volume" },
      "all_warehouses": { 
          "$push": { 
              "warehouse_id": "$_id", 
              "total_successful_orders": "$total_successful_orders",
              "total_sales_volume": "$total_sales_volume" 
          } 
      }
  }},
  { "$unwind": "$all_warehouses" },
  { "$match": { 
      "$expr": { "$eq": [ "$all_warehouses.total_sales_volume", "$max_sales" ] } 
  }},
  { "$lookup": { 
      "from": "warehouses", 
      "localField": "all_warehouses.warehouse_id", 
      "foreignField": "_id", 
      "as": "warehouse_data" 
  }},
  { "$unwind": "$warehouse_data" },
  { "$project": { 
      "_id": 0, 
      "warehouse_name": "$warehouse_data.name", 
      "city": "$warehouse_data.city", 
      "total_successful_orders": "$all_warehouses.total_successful_orders",
      "total_sales_volume": "$all_warehouses.total_sales_volume" 
  }}
])


//top 3 orase cu reviewuri 
db.products.aggregate([
  { "$unwind": "$reviews" },
  { "$group": { 
      "_id": "$reviews.user_pg_id", 
      "review_count": { "$sum": 1 } 
  }},
  { "$lookup": {
      "from": "users",
      "localField": "_id",
      "foreignField": "pg_id",
      "as": "user_data"
  }},
  { "$unwind": "$user_data" },
  { "$unwind": "$user_data.addresses" },
  { "$group": {
      "_id": "$user_data.addresses.city",
      "total_reviews": { "$sum": "$review_count" }
  }},
  { "$sort": { "total_reviews": -1 } },
  { "$limit": 3 },
  { "$project": {
      "_id": 0,
      "city": "$_id",
      "total_reviews": 1
  }}
])

///interogare suplimentara, useri care au cheltuit peste userul cu id 1
db.orders.aggregate([
  { "$group": { 
      "_id": "$user_id", 
      "total_spent": { "$sum": "$total" } 
  }},
  { "$group": {
      "_id": null,
      "all_users": { "$push": { "user_id": "$_id", "total_spent": "$total_spent" } }
  }},
  { "$addFields": {
      "user_one": {
          "$filter": {
              "input": "$all_users",
              "as": "u",
              "cond": { "$eq": ["$$u.user_id", 1] }
          }
      }
  }},
  { "$addFields": {
      "user_one_spent": { "$arrayElemAt": ["$user_one.total_spent", 0] }
  }},
  { "$unwind": "$all_users" },
  { "$match": { 
      "$expr": { "$gt": [ "$all_users.total_spent", "$user_one_spent" ] } 
  }},
  { "$lookup": { 
      "from": "users", 
      "localField": "all_users.user_id", 
      "foreignField": "_id", 
      "as": "user" 
  }},
  { "$unwind": "$user" },
  { "$project": { 
      "_id": 0, 
      "first_name": "$user.first_name", 
      "last_name": "$user.last_name", 
      "total_spent": "$all_users.total_spent" 
  }}
])
