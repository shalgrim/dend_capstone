select count(distinct immigration_id), i94mon
from fact_immigration
group by i94mon
order by i94mon;  -- perfect

select count(1) from fact_immigration where i94yr is null or i94yr <> 2016;

select cicid, count(cicid) from fact_immigration group by cicid order by count desc;

-- over 500 cicids appear 12 times...let's look at a handful to see if they are duplicates
select * from fact_immigration
where cicid in (5454856, 3334634, 4087143, 395680)
order by cicid; -- those are very clearly not duplicates

UPDATE fact_immigration
SET i94bir = NULL
WHERE i94bir < 0 or i94bir > 120;

UPDATE fact_immigration
SET biryear = NULL
WHERE biryear < 1900 or biryear > 2016;

select count(count) N, count
from fact_immigration
group by count
order by N desc;
