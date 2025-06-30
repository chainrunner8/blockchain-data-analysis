with tps as (
  select
    date_trunc(second, block_timestamp) as sec
    , count(*) as txns
  from monad.testnet.fact_transactions
  where block_timestamp between '2025-02-19' and (date_trunc(hour, sysdate() - interval '1 hour') - interval '1 second')
  and block_timestamp not between '2025-03-20 12:00:00' and '2025-03-20 15:00:00'
  group by 1
)

, seconds_table as (
  select dateadd(second, seq4(), '2025-02-19 15:00:00') as sec
  from table(generator(rowcount => 2756 * 60 * 60 - 3600))
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
  sec
  , txns
from real_tps
where txns >= 10000
order by 2 desc
