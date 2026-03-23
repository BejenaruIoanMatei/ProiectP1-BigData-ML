
-------- nr maxim de reviews ---
SELECT product_id, COUNT(*) AS nr_review 
FROM dev.reviews 
GROUP BY product_id 
ORDER BY nr_review DESC 
LIMIT 1;

---- nr de useri care au cheltuit peste medie---
WITH UserTotals AS (
    SELECT user_id, SUM(total) as total_spent
    FROM dev.carts
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

---produse cu pret > 100 si rating 5 
SELECT DISTINCT 
    p.title, 
    p.price
FROM dev.products p
JOIN dev.reviews r ON p.id = r.product_id
WHERE p.price > 100 AND r.rating = 5
ORDER BY p.price DESC;

--- utilizatori care au cumparat cele mai multe produse unice (top 5)
SELECT 
    u.email, 
    COUNT(DISTINCT ci.product_id) as tipuri_produse_unice
FROM dev.users u
JOIN dev.carts c ON u.id = c.user_id
JOIN dev.cart_items ci ON c.id = ci.cart_id
GROUP BY u.email
ORDER BY tipuri_produse_unice DESC
LIMIT 5;