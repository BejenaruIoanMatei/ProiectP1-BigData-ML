// 1. Produsul cu numărul maxim de review-uri
db.products_catalog.aggregate([
    { $project: { title: 1, nr_review: { $size: { $ifNull: ["$reviews", []] } } } },
    { $sort: { nr_review: -1 } },
    { $limit: 1 }
]);

// ---------------------------------------------------------

// 2. Utilizatori care au cheltuit peste media generală
db.orders.aggregate([
    { $group: { _id: "$customer.email", totalSpent: { $sum: "$total_price" } } },

    {
        $group: {
            _id: null,
            avgGlobal: { $avg: "$totalSpent" },
            allUsers: { $push: { email: "$_id", totalSpent: "$totalSpent" } }
        }
    },

    { $unwind: "$allUsers" },
    
    {
        $match: {
            $expr: { $gt: ["$allUsers.totalSpent", "$avgGlobal"] }
        }
    },
    
    {
        $project: {
            _id: 0,
            email: "$allUsers.email",
            totalSpent: "$allUsers.totalSpent",
            mediaGenerala: "$avgGlobal"
        }
    },
    { $sort: { totalSpent: -1 } }
]);

// ---------------------------------------------------------

db.products_catalog.aggregate([
    { 
        $match: { 
            price: { $gt: 100 },
            "reviews.rating": 5 
        } 
    },
    { $project: { title: 1, price: 1, _id: 0 } },
    { $sort: { price: -1 } }
]);

// ---------------------------------------------------------
db.orders.aggregate([
    { $unwind: "$items" }, 
    { 
        $group: { 
            _id: "$customer.email", 
            produse_unice: { $addToSet: "$items.product_id" } 
        } 
    },
    { 
        $project: { 
            email: "$_id", 
            tipuri_produse_unice: { $size: "$produse_unice" } 
        } 
    },
    { $sort: { tipuri_produse_unice: -1 } },
    { $limit: 5 }
]);