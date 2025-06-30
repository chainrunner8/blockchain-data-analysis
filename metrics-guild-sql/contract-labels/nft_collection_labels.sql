-- by @chainrunner_ (Monad Metrics Guild)
-- please give due credit when forking/querying

-- FLIPSIDE SUNSETTING: this code is obsolete because livequery cannot be used anymore.
-- Adapting this code with Python (google api client & auth lib) is an option.


with 

liveqry as (
  select
    livequery.live.udf_api(
      'GET',
      'https://science.flipsidecrypto.xyz/googlesheets/readsheet',
      { 'Content-Type': 'application/json' },
      { 'sheets_id': '1_AX3JinHuNh4b6xh6_xQTdhE_di-02R7Gm73mgTGSpE',
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
  lower(d.value:"address" :: varchar) as address,
  d.value:"name" :: varchar as name
from
  json_data,
  lateral flatten(input => json_data.result :: variant) d
