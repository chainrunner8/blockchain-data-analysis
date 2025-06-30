-- by @chainrunner_

with 

trending_contracts as (
  select * from $query('2bc7dc97-d2ae-4744-bc3e-20fc230d46e1')  -- trending contracts table
)

, top_contracts as (
  select
    date_trunc(day, block_timestamp) as day_
    , contract
    , category
    , count(*) as tx_count
    , count(distinct from_address) as dau
    , round(sum(coalesce((max_priority_fee_per_gas * gas_used) / 1e9, 0)), 1) as priority_fees
  from trending_contracts left join monad.testnet.fact_transactions on contract_address = to_address
  where 1=1
    and block_timestamp between date_trunc(day, sysdate() - interval '7 days') and (date_trunc(day, sysdate()) - interval '1 second')
    and tx_succeeded
    and category != 'Token'
  group by 1, 2, 3
  qualify row_number() over (partition by day_ order by {{daily_ranking}} desc) <= 3
)

select
  day_
  , leaders[0] as "First 🥇"
  , leaders[1] as "Second 🥈"
  , leaders[2] as "Third 🥉"
from (
  select
    day_
    , array_agg(
        object_construct(
          'contract'
          , contract
          , 'category'
          , category
          , 'tx count'
          , tx_count
          , 'daily unique users'
          , dau
          , 'priority fees'
          , concat(priority_fees, ' MON')
        )
      ) as leaders
  from top_contracts
  group by 1
)
order by 1
