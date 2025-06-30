-- forked from @pepperatzi / Top Contracts Based on Gas Fees @ https://flipsidecrypto.xyz/pepperatzi/q/XrrQmfkcT-Ms/top-contracts-based-on-gas-fees
-- by @chainrunner_


with 

labelled_contracts as (
  select * from $query('ae1af2d4-a9d1-4cad-a095-6c6a6c33fae4')  -- labelled contracts table
)

, contracts as (
 select * from $query('2bc7dc97-d2ae-4744-bc3e-20fc230d46e1')  -- trending contracts table
)

, base as (
  select
    block_timestamp
    , from_address as user_
    , contract
    , tx_fee
    -- , coalesce((max_priority_fee_per_gas * gas_used) / 1e9, 0) as priority_fee
  from monad.testnet.fact_transactions right join contracts on to_address = contract_address
  where 1=1
    and block_timestamp >= '2025-02-19 15:00:00'
    and tx_succeeded
)

, agg as (
  select
    contract
    , count(*) as tx_count
    , count(distinct user_) as alltime_uw
    , round(sum(tx_fee), 1) as total_fees
    -- , round(sum(priority_fee), 1) as total_pfee
    -- , round(sum(priority_fee) / alltime_uw, 4) as pfee_by_uw
  from base
  group by 1
)

select
  contract  -- 1
  , coalesce(min(category), 'NA') as category -- 2
  , tx_count  -- 3
  , alltime_uw  -- 4
  , round(avg_duw) as avg_duw  -- 5
  , total_fees  -- 6
  -- , total_pfee  -- 7
  -- , pfee_by_uw  -- 8
from agg
  left join (
    select
      contract
      , sum(duw) / datediff(days, '2025-02-19 15:00:00', sysdate()) as avg_duw
    from (
      select
        contract
        , date_trunc(day, block_timestamp) as day_
        , count(distinct user_) as duw
      from base
      group by 1, 2
    )
    group by 1
  ) using(contract)
  left join labelled_contracts on contract = name  -- for category column
group by 1, 3, 4, 5, 6--, 7, 8
order by 3 desc
