select
  date_trunc(hour, block_timestamp) as hour_
  , count(*) as nb_blocks
  , round(3600 / nb_blocks, 2) as avg_block_time
from monad.testnet.fact_blocks
where block_timestamp between (date_trunc(hour, sysdate() - interval '7 days')) and (date_trunc(hour, sysdate()) - interval '1 hour' - interval '1 second')
group by 1
order by 1
