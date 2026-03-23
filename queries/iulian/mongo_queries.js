
// Top 5 produse dupa pret si rating mediu
use('big_data_db');
db.products_catalog.aggregate([
  { $addFields: {
    avg_rating: {
      $round: [
        { $avg: "$reviews.rating" },
        2
      ]
    },
    nr_recenzii: {
      $size: "$reviews"
    }
  }},
  { $match: {
    nr_recenzii: { $gte: 1 }
  }},
  { $sort: {
    price: -1,
    avg_rating: -1
  }},
  { $limit: 5 },
  { $project: {
    _id: 0,
    title: 1, price: 1,
    avg_rating: 1,
    nr_recenzii: 1
  }}
]);

// Utilizatori cu cele mai multe produse unice cumparate
use('big_data_db');
db.orders.aggregate([
  { $match: {
    "customer.name": {
      $ne: "Unknown User"
    }
  }},
  { $unwind: "$items" },
  { $group: {
    _id: "$customer.email",
    name: {
      $first: "$customer.name"
    },
    produse_unice: {
      $addToSet:
        "$items.product_name"
    }
  }},
  { $addFields: {
    nr_produse_unice: {
      $size: "$produse_unice"
    }
  }},
  { $sort: {
    nr_produse_unice: -1
  }},
  { $limit: 10 },
  { $project: {
    _id: 0,
    name: 1,
    email: "$_id",
    nr_produse_unice: 1
  }}
]);

// Recenzenti nemultumiti (rating ≤ 2)
use('big_data_db');
db.products_catalog.aggregate([
  { $unwind: "$reviews" },
  { $match: {
    "reviews.rating": {
      $lte: 2
    }
  }},
  { $sort: {
    "reviews.rating": 1
  }},
  { $project: {
    _id: 0,
    produs: "$title",
    recenzent: "$reviews.user",
    rating: "$reviews.rating",
    comentariu: "$reviews.comment"
  }}
]);

// Clienti inactivi (fara niciun cart)
use('big_data_db');
db.products_catalog.aggregate([
    { $lookup: {
        from: "orders",
        localField: "title",
        foreignField: "items.product_name",
        as: "comenzi"
    }},
    { $match: {
        comenzi: { $size: 0 }
    }},
    { $project: {
        _id: 0,
        title: 1,
        price: 1
    }}
]);

// Care sunt cele mai "vânate" produse?
use('big_data_db');
db.orders.aggregate([
    { $unwind: "$items" },
    { $group: {
        _id: "$items.product_name", 
        total_unitati_vandute: { $sum: "$items.quantity" },
        venit_total_produs: { $sum: { $multiply: ["$items.quantity", "$items.unit_price"] } },
        numar_aparitii_in_carturi: { $sum: 1 }
    }},
    { $sort: { total_unitati_vandute: -1 } },
    { $limit: 5 },
    { $project: {
        _id: 0,
        produs: "$_id",
        total_unitati_vandute: 1,
        venit_generat: { $round: ["$venit_total_produs", 2] }
    }}
]);