with

base as (
  select
    round(percentile_disc(0.95) within group (order by tx_fee), 4) as perct_95
  from monad.testnet.fact_transactions
  where block_timestamp between (date_trunc(hour, sysdate() - interval '7 days')) and (date_trunc(hour, sysdate()) - interval '1 hour' - interval '1 second')
)

, nft_list as (
  select
    *
    , 'NFT' as category
  from $query('65bc078e-295d-4670-a793-ed4fbdfa29d1')  -- contract-labels/nft_collection_labels.sql
)

, others_list as (
  select * from $query('ae1af2d4-a9d1-4cad-a095-6c6a6c33fae4')  -- contract-labels/contract_labels.sql
)

, contracts as (
  select * from others_list left join nft_list using (address, name, category)
)

, expensive_txns as (
  select
    to_address
    , tx_hash
    --, tx_fee
  from monad.testnet.fact_transactions
  where tx_fee >= (select perct_95 from base)
  and (block_timestamp between (date_trunc(hour, sysdate() - interval '7 days')) and (date_trunc(hour, sysdate()) - interval '1 hour' - interval '1 second'))
  and to_address is not null
)

, pre_final as (
  select
    coalesce(name, to_address) as contract
    , tx_hash
    --, tx_fee
  from expensive_txns
  left join contracts on address = to_address
  --where tx_fee is not null
)

select
  contract
  , round(sum(tx_fee), 2) as total_fees
  , count(*) as n_txn
from pre_final
group by 1
order by 3 desc
