

// Top 5 produse dupa pret si rating mediu
use('big_data_db');
db.products_catalog.aggregate([
  { $lookup: {
      from: "reviews",
      localField: "_id",
      foreignField: "product_id",
      as: "reviews"
  }},
  { $match: {
      "reviews.0": { $exists: true }
  }},
  { $addFields: {
      avg_rating: {
        $round: [
          { $avg: "$reviews.rating" },
          2
        ]
      },
      nr_recenzii: { $size: "$reviews" }
  }},
  { $sort: {
      price: -1,
      avg_rating: -1
  }},
  { $limit: 5 },
  { $project: {
      _id: 0,
      title: 1,
      price: 1,
      avg_rating: 1,
      nr_recenzii: 1
  }}
]);

// Cati utilizatori au cumparat cel mai mare numar de produse unice
use('big_data_db');
db.users.aggregate([
  { $match: {
      first_name: { $ne: "Unknown" }
  }},
  { $lookup: {
      from: "carts",
      localField: "_id",
      foreignField: "user_id",
      as: "carts"
  }},
  { $unwind: "$carts" },
  { $lookup: {
      from: "cart_items",
      localField: "carts._id",
      foreignField: "cart_id",
      as: "items"
  }},
  { $unwind: "$items" },
  { $group: {
      _id: "$_id",
      first_name: { $first: "$first_name" },
      last_name:  { $first: "$last_name"  },
      email:      { $first: "$email"      },
      produse_unice: {
        $addToSet: "$items.product_id"
      }
  }},
  { $addFields: {
      produse_unice: {
        $size: "$produse_unice"
      }
  }},
  { $sort: { produse_unice: -1 }},
  { $limit: 10 },
  { $project: {
      _id: 0,
      first_name: 1, last_name: 1,
      email: 1, produse_unice: 1
  }}
]);

// Recenzenti nemultumiti si produsele pe care le-au recenzat
use('big_data_db');
db.reviews.aggregate([
  { $match: {
      rating: { $lte: 2 }
  }},
  { $lookup: {
      from: "products_catalog",
      localField: "product_id",
      foreignField: "_id",
      as: "product"
  }},
  { $unwind: "$product" },
  { $sort: {
      rating: 1,
      date: -1
  }},
  { $project: {
      _id: 0,
      reviewer_name: 1,
      produs: "$product.title",
      rating: 1,
      comment: 1,
      date: 1
  }}
]);

// Utilizatori care nu au creat niciodata un cart
use('big_data_db');
db.users.aggregate([
  { $match: {
      first_name: { $ne: "Unknown" }
  }},
  { $lookup: {
      from: "carts",
      localField: "_id",
      foreignField: "user_id",
      as: "carts"
  }},
  { $match: {
      carts: { $size: 0 }
  }},
  { $sort: { last_name: 1 }},
  { $project: {
      _id: 1,
      first_name: 1,
      last_name: 1,
      email: 1
  }}
]);