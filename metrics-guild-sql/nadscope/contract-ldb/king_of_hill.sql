-- by @chainrunner_

with 

trending_contracts as (
  select * from $query('2bc7dc97-d2ae-4744-bc3e-20fc230d46e1')  -- trending contracts table
)

, top_contracts as (
  select
    contract
    , category
    , count(*) as tx_count
    , count(distinct from_address) as dau
    , round(sum(coalesce((max_priority_fee_per_gas * gas_used) / 1e9, 0)), 1) as  priority_fees
    , row_number() over (order by {{king_ranking}} desc) as rank
  from trending_contracts left join monad.testnet.fact_transactions on contract_address = to_address
  where 1=1
    and block_timestamp >= date_trunc(day, sysdate())
    and tx_succeeded
    and category != 'Token'
  group by 1, 2
)

select
  concat(
  'Number '
  , rank
  , case
      when rank = 1 then ' 🥇'
      when rank = 2 then ' 🥈'
      when rank = 3 then ' 🥉'
    end
  ) as rank
  , contract
  , category
  , {{king_ranking}}
from top_contracts
where 1=1
  and rank <= 3
