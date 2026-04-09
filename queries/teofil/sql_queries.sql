---- nr de useri care au cheltuit peste medie---
WITH UserTotals AS (
    SELECT user_id, SUM(total) as total_spent
    FROM dev.orders
    GROUP BY user_id
)
SELECT 
    u.first_name, 
    u.last_name, 
    ut.total_spent
FROM dev.users u
JOIN UserTotals ut ON u.id = ut.user_id
WHERE ut.total_spent > (SELECT AVG(total_spent) FROM UserTotals)
ORDER BY ut.total_spent DESC;

--- depozitele in care s-a platit cel mai mult cu cash --- 
WITH WarehouseCardRevenue AS (
    SELECT 
        w.name AS warehouse_name,
        w.city,
        COUNT(p.id) AS total_card_transactions,
        SUM(p.amount) AS total_revenue_from_cards
    FROM dev.warehouses w
    INNER JOIN dev.shipments s ON w.id = s.origin_warehouse_id
    INNER JOIN dev.orders o ON s.order_id = o.id
    INNER JOIN dev.payments p ON o.id = p.order_id
    WHERE p.method = 'cash' 
      AND p.status = 'completed'
    GROUP BY 
        w.id, w.name, w.city
),
RankedWarehouses AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY total_revenue_from_cards DESC) AS revenue_rank
    FROM WarehouseCardRevenue
)

SELECT 
    warehouse_name,
    city,
    total_card_transactions,
    total_revenue_from_cards
FROM RankedWarehouses
WHERE revenue_rank = 1;

--- depozitele cu cele mai mari vanzari --- 
WITH WarehouseSales AS (
    SELECT 
        w.name AS warehouse_name,
        w.city,
        COUNT(DISTINCT o.id) AS total_successful_orders,
        SUM(o.discounted_total) AS total_sales_volume
    FROM dev.warehouses w
    INNER JOIN dev.shipments s ON w.id = s.origin_warehouse_id
    INNER JOIN dev.orders o ON s.order_id = o.id
    WHERE o.status IN ('paid', 'shipped', 'delivered')
    GROUP BY 
        w.id, w.name, w.city
),
RankedSales AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY total_sales_volume DESC) AS sales_rank
    FROM WarehouseSales
)
SELECT 
    warehouse_name,
    city,
    total_successful_orders,
    total_sales_volume
FROM RankedSales
WHERE sales_rank = 1;

--- top 3 orase cu cele mai multe reviewuri --- 

WITH CountryReviewCounts AS (
    SELECT 
        a.city,
        COUNT(r.id) AS total_reviews
    FROM dev.reviews r
    INNER JOIN dev.users u ON r.user_id = u.id
    INNER JOIN dev.addresses a ON u.id = a.user_id
    GROUP BY 
        a.city
),
RankedCountries AS (
    SELECT 
        city,
        total_reviews,
        DENSE_RANK() OVER (ORDER BY total_reviews DESC) AS rank_position
    FROM CountryReviewCounts
)
SELECT 
    city,
    total_reviews,
    rank_position
FROM RankedCountries
WHERE rank_position <= 3;

--- useri care au cheltuit peste user 1 ---
--- interogare suplimentara --- 
WITH UserTotals AS (
    SELECT user_id, SUM(total) as total_spent
    FROM dev.orders
    GROUP BY user_id
),
UserOneTotal AS (
    SELECT total_spent
    FROM UserTotals
    WHERE user_id = 1
)
SELECT 
    u.first_name, 
    u.last_name, 
    ut.total_spent
FROM dev.users u
JOIN UserTotals ut ON u.id = ut.user_id
WHERE ut.total_spent > (SELECT total_spent FROM UserOneTotal)
ORDER BY ut.total_spent DESC;
