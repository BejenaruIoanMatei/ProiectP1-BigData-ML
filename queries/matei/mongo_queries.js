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