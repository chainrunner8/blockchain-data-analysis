with 

first_3_days as (
select
  count(*) as first3_txns
  , count(distinct from_address) as first3_unique_users
from monad.testnet.fact_transactions
where 1=1
  and block_timestamp between '2025-02-19 15:00:00' and dateadd(day, 3, '2025-02-19 15:00:00')
  and tx_succeeded
)

, f3_contracts as (
select
  count(distinct address) as f3_contracts
from monad.testnet.dim_contracts
where 1=1
  and created_block_timestamp between '2025-02-19 15:00:00' and dateadd(day, 3, '2025-02-19 15:00:00')
)

, last_3_days as (
select
  count(*) as last3_txns
  , count(distinct from_address) as last3_unique_users
from monad.testnet.fact_transactions, first_3_days
where 1=1
  and block_timestamp between dateadd(day, -3, sysdate()) and sysdate()
  and tx_succeeded
)

, l3_contracts as (
select
  count(distinct address) as l3_contracts
from monad.testnet.dim_contracts
where 1=1
  and created_block_timestamp between dateadd(day, -3, sysdate()) and sysdate()
)


select
first3_txns
, first3_unique_users
, f3_contracts
, last3_txns
, last3_unique_users
, l3_contracts
, round(last3_txns / first3_txns)
, round(last3_unique_users / first3_unique_users)
, round(l3_contracts / f3_contracts)
from last_3_days, first_3_days, f3_contracts, l3_contracts
