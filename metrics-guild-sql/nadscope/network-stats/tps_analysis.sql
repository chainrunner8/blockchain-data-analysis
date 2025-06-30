with tps as (
  select
    date_trunc(second, block_timestamp) as sec
    , count(*) as txns
  from monad.testnet.fact_transactions
  where block_timestamp between date_trunc(hour, sysdate()) - interval '7 days' and (date_trunc(hour, sysdate() - interval '1 hour') - interval '1 second')
  and block_timestamp not between '2025-03-20 12:00:00' and '2025-03-20 15:00:00'
  group by 1
)

, seconds_table as (
  select dateadd(second, seq4(), date_trunc(hour, sysdate()) - interval '6 days') as sec
  from table(generator(rowcount => 6 * 24 * 60 * 60 - 3600))
)

, real_tps as (
  select
    sec
    , coalesce(txns, 0) as txns
  from seconds_table
  left join tps using(sec)
  where sec < date_trunc(hour, sysdate() - interval '1 hour')
)

select
  date_trunc(hour, sec) as hour_
  , median(txns) as median_tps
  , percentile_disc(0.95) within group (order by txns) as "95th percentile"
  , percentile_disc(0.05) within group (order by txns) as "5th percentile"
  , max(txns) as "Max TPS"
from real_tps
group by 1
order by 1 desc
