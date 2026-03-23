// 1. Top 5 utilizatori care au cheltuit cele mai mari sume de bani
use('big_data_db');
db.orders.aggregate([
    { $match: { "customer.name": { $ne: "Unknown User"} } },
    { $group: {
        _id: "$customer.email",
        totalSpent: { $sum: "$total_price"},
        name: { $first: "$customer.name"}
    }},
    { $sort: { totalSpent: -1}},
    { $limit: 5}
]);

// 2. Toate produsele care au o medie a rating ului mai mare
//de 4.0 si care au cel putin 2 review uri
use('big_data_db');
db.products_catalog.aggregate([
    { $project: 
        {
            title: 1,
            avgRating: { $avg: "$reviews.rating"},
            numReviews: { $size: "$reviews"}
        }
    },
    { $match: 
        {
            avgRating: { $gt: 4.0},
            numReviews: { $gte: 2}
        }
    },
    { $sort: { avgRating: -1}}
]);

// 3. Cat de agresive sunt reducerile ?
//pentru fiecare cart -> valoarea economisita (dif dintre 
//total si discounted)
use('big_data_db');
db.orders.aggregate([
    { $addFields: 
        {
            dif_discount: { $subtract: ["$total_price", "$discounted_total"]}
        }
    },
    {
        $match:
        {
            dif_discount: { $gt: 50}
        }
    },
    { $project:
        {
            "customer.email":1,
            dif_discount:1,
            _id:0
        }
    }
]);

// 4. Cate produse "fantoma" sunt ?
//fantoma = produsele care nu au fost adaugate niciodata
//in niciun cart
use('big_data_db');
db.products_catalog.aggregate([
    { $lookup:
        {
            from: "orders",
            localField: "title",
            foreignField: "items.product_name",
            as: "pc_o"
        }
    },
    { $match:
        {
            pc_o: { $size: 0}
        }
    },
    { $project:
        {
            title:1
        }
    }
]);