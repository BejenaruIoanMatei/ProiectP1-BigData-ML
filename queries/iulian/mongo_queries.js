// Top curieri dupa numarul de livrari
db.orders.aggregate([
  { $group: {
      _id: "$shipment.courier_id",
      nr_livrari: { $sum: 1 }
  }},
  { $lookup: {
      from: "couriers",
      localField: "_id",
      foreignField: "_id",
      as: "curier"
  }},
  { $unwind: "$curier" },
  { $project: {
      curier: "$curier.full_name",
      companie: "$curier.company.name",
      nr_livrari: 1
  }},
  { $sort: { nr_livrari: -1 } }
])

// Evoluția lunară a vânzărilor față de luna anterioară
db.orders.aggregate([
  { $group: {
      _id: { $dateToString: { format: "%Y-%m", date: "$created_at" } },
      nr_comenzi: { $sum: 1 },
      venituri: { $sum: "$discounted_total" }
  }},
  { $sort: { _id: 1 } },
  { $setWindowFields: {
      sortBy: { _id: 1 },
      output: {
        venituri_luna_anterioara: {
          $shift: { output: "$venituri", by: -1 }
        }
      }
  }},
  { $project: {
      luna: "$_id",
      nr_comenzi: 1,
      venituri: { $round: ["$venituri", 2] },
      venituri_luna_anterioara: { $round: ["$venituri_luna_anterioara", 2] },
      diferenta: { $round: [{ $subtract: ["$venituri", "$venituri_luna_anterioara"] }, 2] }
  }}
])

// Statusul livrărilor per curier
db.orders.aggregate([
  { $unwind: "$shipment.status_history" },
  { $group: {
      _id: {
        courier_id: "$shipment.courier_id",
        status: "$shipment.status_history.status"
      },
      count: { $sum: 1 }
  }},
  { $group: {
      _id: "$_id.courier_id",
      statusuri: { $push: { status: "$_id.status", count: "$count" } },
      total: { $sum: "$count" }
  }},
  { $lookup: {
      from: "couriers",
      localField: "_id",
      foreignField: "_id",
      as: "curier"
  }},
  { $unwind: "$curier" },
  { $project: {
      curier: "$curier.full_name",
      livrate:     { $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "delivered"] } } }, in: "$$this.count" } } },
      in_tranzit:  { $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "in_transit"] } } }, in: "$$this.count" } } },
      in_asteptare:{ $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "pending"] } } }, in: "$$this.count" } } },
      respinse:    { $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "rejected"] } } }, in: "$$this.count" } } },
      total: 1
  }},
  { $sort: { livrate: -1 } }
])

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
