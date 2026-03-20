SELECT *
FROM dev.users;

SELECT 
    COUNT(*)
FROM dev.users;
-- Res: 73

SELECT
    COUNT(*) as unknown_number
FROM dev.users
WHERE first_name = 'Unknown';
-- Res: 23

SELECT *
FROM dev.carts;

SELECT *
FROM dev.products;

SELECT *
FROM dev.cart_items;

SELECT *
FROM dev.reviews;