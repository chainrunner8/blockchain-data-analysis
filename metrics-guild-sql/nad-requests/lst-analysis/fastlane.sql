with

fastlane_DP as (
select
block_timestamp
, origin_from_address as signer
, regexp_substr_all(substr(data, 3), '.{64}') as segmented
, livequery.utils.udf_hex_to_int(segmented[0]) / '1e18' as amount_in
, livequery.utils.udf_hex_to_int(segmented[1]) / '1e18' as amount_out
from monad.testnet.fact_event_logs
where contract_address = lower('0x3a98250F98Dd388C211206983453837C8365BDc1')
and topic_0 = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
and block_timestamp > '2025-02-19 15:00:00'
)

, daily_DP as (
select
date_trunc(day, block_timestamp) as day
--, count(distinct signer) as unique_wallets
, sum(amount_out) as deposit_sum
from fastlane_DP
group by 1
)

, fastlane_WD as (
select
block_timestamp
, origin_from_address as signer
, regexp_substr_all(substr(data, 3), '.{64}') as segmented
, livequery.utils.udf_hex_to_int(segmented[0]) / '1e18' as amount_redeemed_mon
, livequery.utils.udf_hex_to_int(segmented[1]) / '1e18' as amount_burned_lst
from monad.testnet.fact_event_logs
where contract_address = lower('0x3a98250F98Dd388C211206983453837C8365BDc1')
and topic_0 = '0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db'
and block_timestamp > '2025-02-19 15:00:00'
)

, daily_WD as (
select
date_trunc(day, block_timestamp) as day
, sum(amount_burned_lst) as withdrawal_sum
from fastlane_WD
group by 1
)

select
day
, deposit_sum - withdrawal_sum as fastlane_net_flow
, sum(fastlane_net_flow) over (order by day asc) as fastlane_circ_supply
from daily_DP
inner join daily_WD using(day)
