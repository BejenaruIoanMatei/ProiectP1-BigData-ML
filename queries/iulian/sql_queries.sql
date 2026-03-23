

-- Top 5 produse dupa pret si rating mediu
SELECT
    p.title,
    p.price,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(r.id) AS nr_recenzii
FROM
    dev.products p
INNER JOIN dev.reviews r ON p.id = r.product_id
GROUP BY
    p.id, p.title, p.price
HAVING
    COUNT(r.id) >= 1
ORDER BY
    p.price DESC, avg_rating DESC
LIMIT 5;

-- Utilizatori cu cele mai multe produse unice cumparate
SELECT
    u.first_name,
    u.last_name,
    u.email,
    COUNT(DISTINCT ci.product_id) AS produse_unice
FROM
    dev.users u
INNER JOIN dev.carts c ON u.id = c.user_id
INNER JOIN dev.cart_items ci ON c.id = ci.cart_id
WHERE
    u.first_name <> 'Unknown'
GROUP BY
    u.id, u.first_name, u.last_name, u.email
ORDER BY
    produse_unice DESC
LIMIT 10;

-- Recenzenti nemultumiti (rating ≤ 2)
SELECT
    r.reviewer_name,
    p.title AS produs,
    r.rating,
    r.comment,
    r.date
FROM
    dev.reviews r
INNER JOIN dev.products p ON r.product_id = p.id
WHERE
    r.rating <= 2
ORDER BY
    r.rating ASC, r.date DESC;

-- Clienti inactivi (fara niciun cart)
SELECT
    u.id,
    u.first_name,
    u.last_name,
    u.email
FROM
    dev.users u
LEFT JOIN dev.carts c ON u.id = c.user_id
WHERE
    c.id IS NULL
    AND u.first_name <> 'Unknown'
ORDER BY
    u.last_name;

