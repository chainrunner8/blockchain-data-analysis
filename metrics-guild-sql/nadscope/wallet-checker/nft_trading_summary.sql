with

nft_labels as (
  select * from $query('65bc078e-295d-4670-a793-ed4fbdfa29d1')
)

, magic_eden_table as (
select
  regexp_substr_all(substr(data, 3), '.{64}') as segments
  , case
      when substr(segments[6], 25) = '760afe86e5de5fa0ee542fc7b7b713e1c5425701'
      then concat('0x', substr(segments[1], 25))
      else concat('0x', substr(segments[14], 25))
    end as seller
  , case
      when substr(segments[6], 25) = '760afe86e5de5fa0ee542fc7b7b713e1c5425701'
      then concat('0x', substr(segments[14], 25))
      else concat('0x', substr(segments[1], 25))
    end as buyer
  , case
      when substr(segments[6], 25) = '760afe86e5de5fa0ee542fc7b7b713e1c5425701'
      then concat('0x', substr(segments[11], 25))
      else concat('0x', substr(segments[6], 25))
    end as collection_addy
  , case
      when substr(segments[6], 25) = '760afe86e5de5fa0ee542fc7b7b713e1c5425701'
      then 
        case
          when array_size(regexp_substr_all('0000000000000000000000000000000000000000000000000000000000000029', '0')) < 55
          then concat('0x', substr(segments[12], 25))
          else livequery.utils.udf_hex_to_int(segments[12])
        end
      else 
        case
          when array_size(regexp_substr_all('0000000000000000000000000000000000000000000000000000000000000029', '0')) < 55
          then concat('0x', substr(segments[7], 25))
          else livequery.utils.udf_hex_to_int(segments[7])
        end
    end as token_id
  , case 
      when substr(segments[6], 25) = '760afe86e5de5fa0ee542fc7b7b713e1c5425701'
      then (livequery.utils.udf_hex_to_int(segments[8]) + livequery.utils.udf_hex_to_int(segments[18])) / 1e18
      else (livequery.utils.udf_hex_to_int(segments[13]) + livequery.utils.udf_hex_to_int(segments[18])) / 1e18
    end as buyer_MON_spent
  , case
      when substr(segments[6], 25) = '760afe86e5de5fa0ee542fc7b7b713e1c5425701'
      then to_double(livequery.utils.udf_hex_to_int(segments[8])) / 1e18
      else to_double(livequery.utils.udf_hex_to_int(segments[13])) / 1e18
    end as seller_MON_received
from monad.testnet.fact_event_logs
where 1=1
and contract_address = '0x0000000000000068f116a894984e2db1123eb395'

and tx_succeeded
and block_timestamp >= '2025-02-19 15:00:00'
)

, wallet_table as (
select
  case
    when seller = lower('{{wallet_address}}')
    then seller_MON_received
    else 0
  end as return
  , case
    when buyer = lower('{{wallet_address}}')
    then buyer_MON_spent
    else 0
  end as investment
  , collection_addy
  , token_id
from magic_eden_table
where 1=1
and lower('{{wallet_address}}') = seller or lower('{{wallet_address}}') = buyer
)


, sub_final as (
select
return
, investment
, coalesce(name, collection_addy) as name 
from wallet_table
left join nft_labels on collection_addy = address
)

select
  sum(return) as total_return
  , sum(investment) as total_investment
  , max(investment) as max_buy
  , max_by(name, investment) as max_buy_collection
  , max(return) as max_sell
  , max_by(name, return) as max_sell_collection
from sub_final
