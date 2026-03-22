use('big_data_db');
db.orders.aggregate([
  { $match: { "customer.name": { $ne: "Unknown Unknown" } } },
  { $group: { 
      _id: "$customer.email", 
      totalSpent: { $sum: "$total_price" },
      name: { $first: "$customer.name" }
  }},
  { $sort: { totalSpent: -1 } },
  { $limit: 5 }
]);