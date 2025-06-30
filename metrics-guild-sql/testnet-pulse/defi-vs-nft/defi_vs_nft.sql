with 

defi_market_vol as (
select * from $query('df384a1f-1da4-4ed5-aacd-7cc5b8f2768e')  -- defi_market_vol.sql
)

, nft_market_vol as (
select * from $query('7f329675-ced0-4d6a-9e47-be601281224f')  -- nft_market_vol.sql
)

select
  day
  , round(d.volume) as defi_volume
  , n.volume as nft_volume
from defi_market_vol d join nft_market_vol n using(day)
order by day
