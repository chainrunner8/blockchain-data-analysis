select
  date_trunc(hour, block_timestamp) as hour_
  , round(median(tx_fee), 4) as median_fee
  , round(percentile_disc(0.95) within group (order by tx_fee), 4) as perct_95
  , round(percentile_disc(0.05) within group (order by tx_fee), 4) as perct_05
from monad.testnet.fact_transactions
where block_timestamp between (date_trunc(hour, sysdate() - interval '7 days')) and (date_trunc(hour, sysdate()) - interval '1 hour' - interval '1 second')
  and tx_succeeded
group by 1
order by 1
