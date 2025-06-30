with

magma_DP as (
  select
    block_timestamp
    , origin_from_address as signer
    , topic_2 / '1e18' as amount_deposited
  from monad.testnet.fact_event_logs
  where contract_address = lower('0x2c9C959516e9AAEdB2C748224a41249202ca8BE7')
  and topic_0 = '0x36af321ec8d3c75236829c5317affd40ddb308863a1236d2d277a4025cccee1e'
  and block_timestamp > '2025-02-19 15:00:00'
)

, daily_DP as (
select
date_trunc(day, block_timestamp) as day
--, count(distinct signer) as unique_wallets
, sum(amount_deposited) as deposit_sum
from magma_DP
group by 1
)

, magma_WD as (
  select
    block_timestamp
    , origin_from_address as signer
    , topic_2 / '1e18' as amount_redeemed
  from monad.testnet.fact_event_logs
  where contract_address = lower('0x2c9C959516e9AAEdB2C748224a41249202ca8BE7')
  and topic_0 = '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568'
  and block_timestamp > '2025-02-19 15:00:00'
)

, daily_WD as (
select
date_trunc(day, block_timestamp) as day
, sum(amount_redeemed) as withdrawal_sum
from magma_WD
group by 1
)

select
day
, deposit_sum - withdrawal_sum as magma_net_flow
, sum(magma_net_flow) over (order by day asc) as magma_circ_supply
from daily_DP
inner join daily_WD using(day)
