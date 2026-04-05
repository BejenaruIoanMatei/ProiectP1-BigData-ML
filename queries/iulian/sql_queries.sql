-- Statusul livrărilor per curier
SELECT
    c.full_name                               AS curier,
    COUNT(ss.id) FILTER (WHERE ss.status = 'delivered')  AS livrate,
    COUNT(ss.id) FILTER (WHERE ss.status = 'in_transit') AS in_tranzit,
    COUNT(ss.id) FILTER (WHERE ss.status = 'pending')    AS in_asteptare,
    COUNT(ss.id) FILTER (WHERE ss.status = 'rejected')   AS respinse,
    ROUND(
        COUNT(ss.id) FILTER (WHERE ss.status = 'delivered') * 100.0
        / NULLIF(COUNT(ss.id), 0)
    , 1)                                      AS rata_succes
FROM dev.couriers c
INNER JOIN dev.shipments s        ON s.courier_id = c.id
INNER JOIN dev.shipment_status ss ON ss.shipment_id = s.id
GROUP BY c.id, c.full_name
ORDER BY rata_succes DESC NULLS LAST;

-- Top curieri cu balotaj

SELECT
    c.full_name                      AS curier,
    cc.name                          AS companie,
    COUNT(s.id)                      AS nr_livrari,
    RANK() OVER (ORDER BY COUNT(s.id) DESC)       AS rank,
    DENSE_RANK() OVER (ORDER BY COUNT(s.id) DESC) AS dense_rank
FROM dev.couriers c
INNER JOIN dev.courier_companies cc ON cc.id = c.company_id
INNER JOIN dev.shipments s          ON s.courier_id = c.id
GROUP BY c.id, c.full_name, cc.name
ORDER BY nr_livrari DESC;

-- Evoluția lunară a vânzărilor
WITH lunar AS (
    SELECT
        DATE_TRUNC('month', created_at)::DATE AS luna,
        COUNT(id)                             AS nr_comenzi,
        ROUND(SUM(discounted_total), 2)       AS venituri
    FROM dev.orders
    GROUP BY DATE_TRUNC('month', created_at)
)
SELECT
    TO_CHAR(luna, 'YYYY-MM')                  AS luna,
    nr_comenzi,
    venituri,
    LAG(venituri) OVER (ORDER BY luna)        AS venituri_luna_anterioara,
    ROUND(venituri - LAG(venituri) OVER (ORDER BY luna), 2) AS diferenta
FROM lunar
ORDER BY luna;

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