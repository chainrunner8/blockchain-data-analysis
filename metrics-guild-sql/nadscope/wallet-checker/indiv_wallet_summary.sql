-- by @chainrunner_

with contract_labels as (
  select * from $query('ae1af2d4-a9d1-4cad-a095-6c6a6c33fae4')  -- trending contracts
)

, contracts as (
  select
    address
      , coalesce(
      case 
        when creator_address = '0xc816865f172d640d93712c68a7e1f83f3fa63235' then 'Kuru'
        when creator_address = '0x7fe0bce62b95b22eb6335b2dac3b4e5a2f6f034e' then 'Nad.fun'
        when creator_address = '0x321fb42877b7c61efe489505ab87bf1dff33f1e4' then 'Nad.fun'
        when creator_address = '0x60216fb3285595f4643f9f7cddab842e799bd642' then 'Nad.fun'
      end
      , l.name
      , d_c.name
      , symbol
      , 'NA'
      ) as contract_name
    , case when contract_name != 'NA' then contract_name else address end as contract
    , coalesce(category, 'NA') as category
  from monad.testnet.dim_contracts d_c
  left join contract_labels l using(address)
)

, contract_interactions as (
  select
    date_trunc(day, block_timestamp)::date as day_
    , contract
    , tx_fee
    , coalesce(max_priority_fee_per_gas * gas_used / 1e9, 0) as prio_fee
  from contracts left join monad.testnet.fact_transactions on address = to_address
  where 1=1
    and block_timestamp >= '2025-02-19 15:00:00'
    and tx_succeeded
    and from_address = lower('{{wallet}}')
)

, transfers as (
  select
    count(distinct to_address) as helped_nads
    , round(sum(value), 2) as total_transferred
  from monad.testnet.fact_traces trc
  where 1=1
    and not exists (
      select 1 from contracts c where trc.to_address = c.address
    )
    and block_timestamp >= '2025-02-19 15:00:00'
    and tx_succeeded
    and from_address = lower('{{wallet}}')
)

, summary_stats as (
  select
    row_number() over (order by null) as index
    , helped_nads
    , total_transferred
    , count(distinct contract) as unique_contracts
    , round(sum(tx_fee), 2) as total_fees_spent
    , round(sum(prio_fee), 3) as total_prio_fees_spent
    , count(distinct day_) as unique_active_days
    , datediff(day, '2025-02-19 15:00:00', date_trunc(day, sysdate())) as testnet_days
  from contract_interactions, transfers
  group by 2, 3
)

, tx_history as (
  select
    day_
    , row_number() over (order by day_) as index
    , count(*) as daily_tx_count
    , sum(daily_tx_count) over (order by day_ desc) as contract_txns
  from contract_interactions
  group by 1
)

, contract_pie as (
  select
    contract
    , count(*) as contract_tx_count
    , sum(tx_fee) as contract_fees_spent
    , sum(prio_fee) as contract_prio_spent
    , count(distinct day_) as contract_active_days
  from contract_interactions
  group by 1
)

, max_tx_contract as (
  select
    'Highest number of txn 🥇' as max_metric
    , contract
    , concat(contract_tx_count, ' txns') as max_value
  from contract_pie 
  qualify row_number() over(order by contract_tx_count desc) = 1
)

, max_fee_contract as (
  select
    'Highest cumulative tx fees 🥇' as max_metric
    , contract
    , concat(round(contract_fees_spent, 2), ' MON') as max_value
  from contract_pie 
  qualify row_number() over(order by contract_fees_spent desc) = 1
)

, max_prio_contract as (
  select
    'Highest cumulative priority fees 🥇' as max_metric
    , contract
    , concat(round(contract_prio_spent, 4), ' MON') as max_value
  from contract_pie
  qualify row_number() over(order by contract_prio_spent desc) = 1
)

, max_days_contract as (
  select
    'Highest number of active days 🥇' as max_metric
    , contract
    , concat(contract_active_days, ' days') as max_value
  from contract_pie
  qualify row_number() over(order by contract_active_days desc) = 1
)

, max_metrics as (
  select
    row_number() over (order by null) as index
    , *
  from (
    select * from max_tx_contract
    union
    select * from max_fee_contract
    union
    select * from max_prio_contract
    union
    select * from max_days_contract
  )
)


select *,
'{{wallet}}' as current_wallet --modified to add the wallet so there is no need to change the code
from tx_history
full outer join max_metrics using(index)
full outer join summary_stats using(index)
full outer join (
  select
    row_number() over (order by null) as index
    , contract as pie_contract
    , contract_tx_count
  from contract_pie
) using(index)
order by index
