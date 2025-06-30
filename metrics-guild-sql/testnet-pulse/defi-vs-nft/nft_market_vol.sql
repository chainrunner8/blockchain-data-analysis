-- FLIPSIDE SUNSETTING: this code is obsolete because livequery cannot be used anymore.
-- Adapting this code with Python (google api client & auth lib) is an option.

with 

liveqry as (
  select
    livequery.live.udf_api(
      'GET',
      'https://science.flipsidecrypto.xyz/googlesheets/readsheet',
      { 'Content-Type': 'application/json' },
      { 'sheets_id': '1HLMrUfzDolROSN81tHSe8BE_30lAwkNWMaWWgkGZ7t8',
      'tab_name': 'Sheet1' }
    ) as result
),
json_data as (
  select
    result:data as result
  from
    liveqry
)
select
  d.value:"volume" :: double * 1000 as volume,
  d.value:"date" :: timestamp as day
from
  json_data,
  lateral flatten(input => json_data.result :: variant) d
