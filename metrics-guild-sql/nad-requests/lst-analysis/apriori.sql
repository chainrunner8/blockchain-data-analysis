with

apriori_DP as (
  select
    block_timestamp
    , origin_from_address as signer
    , regexp_substr_all(substr(data, 3), '.{64}') as segmented
    , livequery.utils.udf_hex_to_int(segmented[0]) / '1e18' as amount_deposited
  from monad.testnet.fact_event_logs
  where contract_address = '0xb2f82d0f38dc453d596ad40a37799446cc89274a'
  and topic_0 = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
  and block_timestamp > '2025-02-19 15:00:00'
)

, daily_DP as (
select
date_trunc(day, block_timestamp) as day
--, count(distinct signer) as unique_wallets
, sum(amount_deposited) as deposit_sum
from apriori_DP
group by 1
)

, apriori_WD as (
  select
    block_timestamp
    , origin_from_address as signer
    , regexp_substr_all(substr(data, 3), '.{64}') as segmented
    , livequery.utils.udf_hex_to_int(segmented[0]) / '1e18' as amount_burned
    , livequery.utils.udf_hex_to_int(segmented[1]) / '1e18' as amount_redeemed
    , amount_burned - amount_redeemed as platform_fee
  from monad.testnet.fact_event_logs
  where contract_address = '0xb2f82d0f38dc453d596ad40a37799446cc89274a'
  and topic_0 = '0x8caf04742286d017f9ac3924388e188c73e6e5094311c5e59a61a7ef86dda8bf'
  and block_timestamp > '2025-02-19 15:00:00'
)

, daily_WD as (
select
date_trunc(day, block_timestamp) as day
, sum(amount_burned) as withdrawal_sum
from apriori_WD
group by 1
)

select
day
, deposit_sum - withdrawal_sum as apriori_net_flow
, sum(apriori_net_flow) over (order by day asc) as apriori_circ_supply
from daily_DP
inner join daily_WD using(day)
