with

kintsu_DP as (
  select
    block_timestamp
    , origin_from_address as signer
    , regexp_substr_all(substr(data, 3), '.{64}') as segmented
    , livequery.utils.udf_hex_to_int(segmented[0]) / '1e18' as amount_out
    , livequery.utils.udf_hex_to_int(segmented[1]) / '1e18' as amount_in
  from monad.testnet.fact_event_logs
  where contract_address = lower('0xe1d2439b75fb9746E7Bc6cB777Ae10AA7f7ef9c5')
  and topic_0 = '0x9b11e540cf3256fce889317399340ba7bd67e98f3d93f016acb7576cd073de70'
  and block_timestamp > '2025-02-19 15:00:00'
)

, daily_DP as (
select
date_trunc(day, block_timestamp) as day
--, count(distinct signer) as unique_wallets
, sum(amount_out) as deposit_sum
from kintsu_DP
group by 1
)

, kintsu_WD as (
  select
    block_timestamp
    , origin_from_address as signer
    , regexp_substr_all(substr(data, 3), '.{64}') as segmented
    , livequery.utils.udf_hex_to_int(segmented[1]) / '1e18' as amount_redeemed
  from monad.testnet.fact_event_logs
  where contract_address = lower('0xe1d2439b75fb9746E7Bc6cB777Ae10AA7f7ef9c5')
  and topic_0 = '0x77c6915c63d8e259aff9f5e533e17e7fca4246257f4689dfd3f2a1df9f8b69ea'
  and block_timestamp > '2025-02-19 15:00:00'
)

, daily_WD as (
select
date_trunc(day, block_timestamp) as day
, sum(amount_redeemed) as withdrawal_sum
from kintsu_WD
group by 1
)

select
day
, deposit_sum - withdrawal_sum as kintsu_net_flow
, sum(kintsu_net_flow) over (order by day asc) as kintsu_circ_supply
from daily_DP
inner join daily_WD using(day)
