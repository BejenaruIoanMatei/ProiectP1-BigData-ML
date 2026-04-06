// 1. Top 5 utilizatori care au cheltuit cele mai mari sume de bani in total
db.orders.find({});
db.orders.aggregate([
    { $lookup: 
        {
            from: "users",
            localField: "user_id",
            foreignField: "_id",
            as: "user"
        }
    },
    { $unwind: "$user"},
    { $match: 
        {
            "user.first_name": { $ne: "Unknown"}
        }
    },
    { $group:
        {
            _id: "$user_id",
            first_name: { $first: '$user.first_name'},
            last_name: { $first: "$user.last_name"},
            total_cheltuit: { $sum: "$discounted_total"}
        }
    },
    { $sort: { total_cheltuit: -1 }},
    { $limit: 5},
    { $project: 
        {
            _id: 0,
            first_name: 1,
            last_name: 1,
            total_cheltuit: { $round: ["$total_cheltuit", 2]}
        }
    }
]);
//-- 2. Pentru fiecare produs sa se calculeze
//--a) Valoarea totala vanduta
//--b) Ratingul mediu din recenzii
//--c) Stocul total disponibil
//--sumat pe toate depozitele.

//-- Pe baza lor sa clasific:
//--a) 'premium' daca avg_rating >= 4.5 si valoarea_vanduta >= 500 
//--b) 'standard' daca avg_rating >= 3.5 sau valoarea_vanduta >= 200
//--c) 'slab' restul
//
//-- Se afiseaza doar produsele cu cel putin o recenzie si cel putin o vanzare
//-- Sortat dupa clasificare si valoarea vanduta descrescator

db.orders.aggregate([
    { $unwind: "$items"},
    { $group: 
        {
            _id: "$items.product_id",
            title: { $first: "$items.title"},
            valoare_vanduta: {
                $sum: { $multiply: ["$items.quantity", "$items.price"]}
            }
        }
    },
    { $lookup: 
        {
            from: "products",
            localField: "_id",
            foreignField: "_id",
            as: "product"
        }
    },
    { $unwind: "$product"},
    { $match: { "product.reviews.0": { $exists: true}}},
    { $addFields: {
        avg_rating: { $avg: "$product.reviews.rating" },
        stoc_disponibil: {
            $subtract: [
                { $sum: "$product.inventory.stock_level" },
                { $sum: "$product.inventory.reserved_quantity" }
            ]
        }
    }},
    { $addFields: {
        clasificare: {
            $cond: {
                if: { $and: [
                    { $gte: ["$avg_rating", 4.5] },
                    { $gte: ["$valoare_vanduta", 500.0] }
                ]},
                then: "premium",
                else: {
                    $cond: {
                        if: { $or: [
                            { $gte: ["$avg_rating", 3.5] },
                            { $gte: ["$valoare_vanduta", 200.0] }
                        ]},
                        then: "standard",
                        else: "slab"
                    }
                }
            }
        }
    }},
    { $sort: { clasificare: 1, valoare_vanduta: -1}},
    { $project: 
        {
            _id: 0,
            title: 1,
            valoare_vanduta: 1,
            avg_rating: 1,
            stoc_disponibil: 1,
            clasificare: 1
        }
    }
]);


//-- Trecem la ceva mai dificil 
//-- 3. Pentru fiecare comanda cu status (paid, shipped, delivered) calculam in felul urmator:
//--a) cu window functions: cheltuiala cumulativa pana la acea comanda (si inclusiv comanda)
//--b) procentul pe care il reprezinta comanda curenta din totalul cheltuielilor utilizatorului
//--c) rangul comenzii ca valoare (rang 1 = cea mai scumpa)
//--Si returnam doar utilizatorii cu minim 2 comenzi valide
//--Sortat dupa user, apoi cronologic

//db.orders.aggregate([
//    { $lookup:
//        {
//            from: "users",
//            localField: "user_id",
//            foreignField: "_id",
//            as: "user"
//        }
//    },
//    { $unwind: "$user"},
//    { $match: 
//        {
//            status: { $in: ['paid', 'shipped', 'delivered']},
//            "user.first_name": { $ne: "Unknown"}
//        }
//    }
//]);

db.orders.aggregate([
    { $lookup:
        {
            from: "users",
            localField: "user_id",
            foreignField: "_id",
            as: "user"
        }
    },
    { $unwind: "$user"},
    { $match: 
        {
            status: { $in: ['paid', 'shipped', 'delivered']},
            "user.first_name": { $ne: "Unknown"}
        }
    },
    { $group:
        {
            _id: "$user_id",
            first_name: { $first: "$user.first_name"},
            last_name: { $first: "$user.last_name"},
            total_user: { $sum: "$discounted_total"},
            nr_comenzi: { $sum: 1},
            comenzi: { $push: 
                {
                    order_id: "$_id",
                    val: "$discounted_total",
                    created_at: "$created_at"
                }
            }
        }
    },
    { $match:
        {
            nr_comenzi: { $gte: 1}
        }
    },
    { $addFields: {
        comenzi: { $sortArray: { input: "$comenzi", sortBy: { created_at: 1 } } }
    }},
    { $addFields:
        {
            rezultat: {
                $reduce: {
                    input: "$comenzi",
                    initialValue: { cumul: 0, rows: []},
                    in: {
                        cumul: {$add: ["$$value.cumul", "$$this.val"]},
                        rows: { $concatArrays: ["$$value.rows", [{
                            order_id: "$$this.order_id",
                            created_at: "$$this.created_at",
                            val: "$$this.val",
                            cumul: { $add: ["$$value.cumul", "$$this.val"]}
                        }]]}
                    }
                }
            }
        }
    },
    { $unwind: "$rezultat.rows"},
    { $project: 
        {
            _id: 0,
            first_name: 1,
            last_name: 1,
            order_id: "$rezultat.rows.order_id",
            created_at: "$rezultat.rows.created_at",
            valoaare_comanda: "$rezultat.rows.val",
            total_cumulativ: "$rezultat.rows.cumul",
            procent_din_total: { $multiply: [{ $divide: ["$rezultat.rows.val", "$total_user"]}, 100]},
            
        }
    },
    {$sort: {first_name: 1, created_at: 1}}
]);


//-- 4. Pentru fiecare oras:
//--a) valoarea totala a comenzilor livrate din depozitele orasului respectiv
//--b) valoarea totala o comparam cu media nationala
//--obtinem ca performanta 2 categorii: peste medie si sub medie

db.orders.aggregate([

  { $match: 
      { 
          status: "delivered" 
      } 
  },

  { $group: 
      {
          _id: "$shipment.warehouse_id",
          nr_comenzi: { $sum: 1 },
          val_depozit:{ $sum: "$discounted_total"}
      }
  },

  { $lookup: 
      {
          from: "warehouses",
          localField:"_id",
          foreignField: "_id",
          as: "warehouse"
      }    
  },
  { $unwind: "$warehouse" },
  { $group: 
      {
          _id: "$warehouse.city",
          nr_depozite:{ $sum: 1 },
          nr_comenzi: { $sum: "$nr_comenzi" },
          valoare_totala:{ $sum:"$val_depozit" }
      }
  },
  { $group: 
      {
          _id: null,
          medie_nationala: { $avg: "$valoare_totala" },
          orase: { $push: {
              oras: "$_id",
              nr_depozite:"$nr_depozite",
              nr_comenzi:"$nr_comenzi",
              valoare_totala: "$valoare_totala"
          }}
      }
  },
  { $unwind: "$orase" },
  { $addFields: {
          diferenta: { $round: [{
                  $subtract: ["$orase.valoare_totala", "$medie_nationala"]
              }, 2]},
          performanta: { $cond: {
              if:{ $gt: ["$orase.valoare_totala", "$medie_nationala"] },
              then: "peste medie",
              else: "sub medie"
          }}
      }
  },
  { $sort: { "orase.valoare_totala": -1 } },
  { $project: {
      _id:0,
      oras:"$orase.oras",
      nr_depozite:"$orase.nr_depozite",
      nr_comenzi: "$orase.nr_comenzi",
      valoare_totala: { $round: ["$orase.valoare_totala", 2]},
      medie_nationala: { $round: ["$medie_nationala", 2]},
      diferenta: 1,
      performanta:1
  }}
]);