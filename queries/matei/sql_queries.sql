
-- 1. Top 5 utilizatori care au cheltuit cele mai mari sume de bani in total

select 
    u.first_name,
    u.last_name,
    sum(c.total) as total_sum
from
    dev.users u 
inner join dev.carts c on u.id = c.user_id
where u.first_name <> 'Unknown'
group by 
    u.first_name,
    u.last_name
order by 
    total_sum desc
limit 5;

-- 2. Toate produsele care au o medie a rating ului mai mare de 4.0
--si care au cel putin 2 review uri

select 
    p.title,
    round(avg(rating), 2) as avg_rating,
    count(*) as n_reviews
from
    dev.products p 
inner join dev.reviews r on p.id = r.product_id
group by
    p.title
having 
    round(avg(rating), 2) > 4.0 
    and count(*) >= 2
order by avg_rating desc;