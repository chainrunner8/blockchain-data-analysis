-- by @chainrunner_ (Monad Metrics Guild)
-- with help from NadRadar
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
       { 
        'sheets_id' : '1V5l77b652QBg9jtatcaxlxgYwwvwY-hMTwc0xyAb3Kc', 
        'tab_name' : 'Sheet1'
      }
    ) as result
)

, json_data as (
select result:data as result from liveqry
)

select
  lower(d.value:"Address"::varchar) as address, 
  d.value:"Name"::varchar as name, 
  d.value:"Category"::varchar as category
from
  json_data,
  lateral flatten(input => json_data.result::variant) d
