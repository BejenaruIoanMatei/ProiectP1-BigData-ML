-- 1. Top 5 utilizatori care au cheltuit cele mai mari sume de bani in total

-- select *
-- from dev.users
-- where first_name = 'Unknown';

select 
    u.first_name,
    u.last_name,
    sum(o.discounted_total) as total_cheltuit
from dev.users u
inner join dev.orders o on u.id = o.user_id
where u.first_name <> 'Unknown'
group by
    u.first_name,
    u.last_name
order by
    total_cheltuit desc
limit 5;

-- 2. Pentru fiecare produs sa se calculeze
--a) Valoarea totala vanduta
--b) Ratingul mediu din recenzii
--c) Stocul total disponibil
--sumat pe toate depozitele.

-- Pe baza lor sa clasific:
--a) 'premium' daca avg_rating >= 4.5 si valoarea_vanduta >= 500 
--b) 'standard' daca avg_rating >= 3.5 sau valoarea_vanduta >= 200
--c) 'slab' restul

-- Se afiseaza doar produsele cu cel putin o recenzie si cel putin o vanzare
-- Sortat dupa clasificare si valoarea vanduta descrescator

select 
    p.title,
    round(sum(oi.quantity * oi.price), 2) as valoare_vanduta,
    round(avg(r.rating), 2) as avg_rating,
    coalesce(sum(i.stock_level - i.reserved_quantity), 0) as stoc_disponibil,
    case
        when avg(r.rating) >= 4.5
            and sum(oi.quantity * oi.price) >= 500 then 'premium'
        when avg(r.rating) >= 3.5
            or sum(oi.quantity * oi.price) >= 200 then 'standard'
        else 'slab'
    end as clasificare
from dev.products p
inner join dev.order_items oi on oi.product_id = p.id
inner join dev.reviews r on r.product_id = p.id
left join dev.inventory i on i.product_id = p.id
group by
    p.title
having
    count(distinct r.id) >= 1 and count(distinct oi.id) >= 1
order by
    clasificare asc,
    valoare_vanduta desc;

-- Trecem la ceva mai dificil 
-- 3. Pentru fiecare comanda cu status (paid, shipped, delivered) calculam in felul urmator:
--a) cu window functions: cheltuiala cumulativa pana la acea comanda (si inclusiv comanda)
--b) procentul pe care il reprezinta comanda curenta din totalul cheltuielilor utilizatorului
--c) rangul comenzii ca valoare (rang 1 = cea mai scumpa)
--Si returnam doar utilizatorii cu minim 2 comenzi valide
--Sortat dupa user, apoi cronologic

-- SELECT status, COUNT(*) 
-- FROM dev.orders 
-- GROUP BY status;

-- SELECT COUNT(*), user_id
-- FROM dev.orders o
-- JOIN dev.users u ON u.id = o.user_id
-- WHERE o.status IN ('paid', 'shipped', 'delivered')
--   AND u.first_name <> 'Unknown'
-- GROUP BY user_id;

-- SELECT COUNT(*) AS useri_eligibili FROM (
--     SELECT user_id
--     FROM dev.orders o
--     JOIN dev.users u ON u.id = o.user_id
--     WHERE o.status IN ('paid', 'shipped', 'delivered')
--       AND u.first_name <> 'Unknown'
--     GROUP BY user_id
--     HAVING COUNT(*) >= 1
-- ) sub;

with comenzi_valide as (
    -- CTE simplu pentru comenzile cu status in (paid, shipped, delivered) cu user real
    select
        o.id as order_id,
        u.id as user_id,
        o.discounted_total,
        o.created_at
    from dev.orders o
    inner join dev.users u on u.id = o.user_id
    where o.status in ('paid', 'shipped', 'delivered') and u.first_name <> 'Unknown'
),
utilizatori_multipli as (
    -- CTE simplu pentru userii care au macar 2 comenzi valide
    select 
        user_id
    from comenzi_valide
    group by user_id
    having count(*) >= 1
)
select
    u.first_name,
    u.last_name,
    cv.order_id,
    cv.created_at,
    round(cv.discounted_total, 2) as valoare_comanda,
    round(sum(cv.discounted_total) over (
        partition by cv.user_id
        order by cv.created_at
    ), 2) as total_cumulativ,
    round(
        cv.discounted_total * 100.0 / nullif(sum(cv.discounted_total) over (
            partition by cv.user_id
        ), 0)
    , 2) as procent_din_total,
    rank() over (
        partition by cv.user_id
        order by cv.discounted_total desc
    ) as rang_valoare
from dev.users u
inner join comenzi_valide cv on u.id = cv.user_id
inner join utilizatori_multipli um on cv.user_id = um.user_id
order by
    cv.user_id,
    cv.created_at;

-- "Wilson"	6	"2025-07-01 11:12:31.784409"	1437.88	1437.88	100.00	1
-- "Baker"	4	"2025-12-22 11:12:31.784393"	84393.57	84393.57	100.00	1
-- "Carter"	3	"2025-06-05 11:12:31.784388"	230.05	230.05	100.00	1

-- Adaugam mai multe comenzi per user

-- insert into dev.orders (id, user_id, cart_id, delivery_address_id, total, discounted_total, status, created_at)
-- select
--     (select max(id) from dev.orders) + row_number() over (),
--     user_id,
--     cart_id,
--     delivery_address_id,
--     total * (0.8 + random() * 0.4),
--     discounted_total * (0.8 + random() * 0.4),
--     (ARRAY['paid', 'shipped', 'delivered'])[floor(random() * 3 + 1)],
--     created_at + (random() * interval '180 days')
-- from dev.orders
-- where status in ('paid', 'shipped', 'delivered');

-- SELECT user_id, COUNT(*) as nr_comenzi
-- FROM dev.orders
-- WHERE status IN ('paid', 'shipped', 'delivered')
-- GROUP BY user_id
-- ORDER BY nr_comenzi DESC;

-- 4. Pentru fiecare oras:
--a) valoarea totala a comenzilor livrate din depozitele orasului respectiv
--b) valoarea totala o comparam cu media nationala
--obtinem ca performanta 2 categorii: peste medie si sub medie

-- with per_oras as (
-- 	select
-- 		w.city as oras,
-- 		count(distinct w.id) as nr_depozite,
-- 		count(distinct o.id) as nr_comenzi,
-- 		round(sum(o.discounted_total)) as valoare_totala
-- 	from dev.warehouses w
-- 	inner join dev.shipments s on s.origin_warehouse_id = w.id
-- 	inner join dev.orders o on o.id = s.order_id
-- 	where o.status = 'delivered'
-- 	group by 
-- 		w.city
-- )
-- select * from per_oras;

-- select count(*) 
-- from dev.orders o
-- join dev.shipments s on s.order_id = o.id
-- where o.status = 'delivered';

with per_oras as (
	select
		w.city as oras,
		count(distinct w.id) as nr_depozite,
		count(distinct o.id) as nr_comenzi,
		round(sum(o.discounted_total)) as valoare_totala
	from dev.warehouses w
	inner join dev.shipments s on s.origin_warehouse_id = w.id
	inner join dev.orders o on o.id = s.order_id
	where o.status = 'delivered'
	group by 
		w.city
)
select 
	oras,
	nr_depozite,
	nr_comenzi,
	valoare_totala,
	round(avg(valoare_totala) over (), 2) as medie_nationala,
	round(valoare_totala - avg(valoare_totala) over (), 2) as dif_fata_de_medie,
	case
		when valoare_totala > avg(valoare_totala) over ()
		then 'peste medie'
		else 'sub medie'
	end as performanta
from per_oras
order by
	valoare_totala desc;