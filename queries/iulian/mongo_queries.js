// Top curieri dupa numarul de livrari
db.orders.aggregate([
    // aflam cate livrari a facut fiecare curier
    { $group: {
        _id: '$shipment.courier_id',
        nr_livrari: { $sum: 1}
    }},
    // aflam id-ul curierului
    { $lookup: {
        from: 'couriers',
        localField: '_id',
        foreignField: '_id',
        as: 'curier'
    }},
    { $unwind: '$curier'},
    // selectam numele curierului, compania la care lucreaza 
    // si numarul de comenzi livrate
    { $project: {
        courier: '$curier.full_name',
        companie: '$curier.company.name',
        nr_livrari: 1
    }},
    // sortam descrescator
    { $sort: {nr_livrari: -1}}
])

// Evoluția lunară a vânzărilor față de luna anterioară
db.orders.aggregate([
    // grupam datele după lună și calculam totalurile
    {$group: {
        _id: {$dateToString: {format: '%Y-%m', date: '$created_at'}},
        nr_comenzi: {$sum:1},
        venituri: {$sum: '$discounted_total'}
    }},
    // ordonam lunile cronologic ca sa pregatim terenul pentru
    // calculul de luna anterioara
    {$sort: { _id: 1}},
    {$setWindowFields: {
        sortBy: { _id: 1},
        output: {
            venituri_luna_anterioara: {
                $shift: {output: '$venituri', by: -1}
            }
        }
    }},
    // Calculam diferența de bani și rotunjim toate cifrele
    {$project: {
        luna: '$_id',
        nr_comenzi: 1,
        venituri: {$round: ['$venituri', 2]},
        venituri_luna_anterioara: {$round: ['$venituri_luna_anterioara', 2]},
        diferenta: {$round: [{$subtract: ['$venituri', '$venituri_luna_anterioara']}, 2]}
    }}
])

// Statusul livrărilor per curier
db.orders.aggregate([
    // analizam fiecare etapă a livrării separat
    {$unwind: '$shipment.status_history'},
    // număram de cate ori apare fiecare status pentru 
    // fiecare curier
    {$group: {
        _id: {
            courier_id: '$shipment.courier_id',
            status: '$shipment.status_history.status'
        },
        count: {$sum: 1}
    }},
    // reunim toate datele sub un singur id de curier si
    // cream o lista cu toate perechile status-număr folosind $push
    {$group: {
        _id: '$_id.courier_id',
        statusuri: {$push: {status: '$_id.status', count: '$count'}},
        total: {$sum: '$count'}
    }},
    // aducem detaliile curierilor
    {$lookup: {
        from: 'couriers',
        localField: '_id',
        foreignField: '_id',
        as: 'curier'
    }},
    {$unwind: '$curier'},
    {$project: {
        curier: '$curier.full_name',
        // extragem doar numărul care corespunde unui anumit status
        livrate:      { $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "delivered"] } } }, in: "$$this.count" } } },
        in_tranzit:   { $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "in_transit"] } } }, in: "$$this.count" } } },
        in_asteptare: { $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "pending"] } } }, in: "$$this.count" } } },
        respinse:     { $sum: { $map: { input: { $filter: { input: "$statusuri", cond: { $eq: ["$$this.status", "rejected"] } } }, in: "$$this.count" } } },
        total: 1
    }},
    // sortam livrarile descrescator
    {$sort: {livrate: -1}}
])

// Top 5 produse dupa pret si rating mediu
db.products.aggregate([
    // filtram doar produsele care au recenzii
    {$match: {
        reviews: {$exists: true, $not: {$size: 0}}
    }},
    {$addFields: {
        // calculam media notelor din lista de recenzii
        avg_rating: {
            $round: [{$avg: '$reviews.rating'}, 2]
        },
        // totalul de recenzii a produsului
        nr_recenzii: {$size: '$reviews'}
    }},
    // sortam dupa pret, iar daca au acelasi pret,
    // sortam dupa rating
    {$sort: {
        price: -1,
        avg_rating: -1
    }},
    {$limit: 5},
    {$project: {
        _id: 0,
        title: 1,
        price: 1,
        avg_rating: 1,
        nr_recenzii: 1
    }}
])