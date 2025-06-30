-- by @chainrunner_

with 

labelled_contracts as (
  select * from $query('ae1af2d4-a9d1-4cad-a095-6c6a6c33fae4')  -- labelled contracts table
)

, smart_filter as (
  select
    c.contract_address
    , count(*) as txns
  from (
      select
        to_address as contract_address
      from monad.testnet.fact_transactions
      where 1=1
        and block_timestamp >= sysdate() - interval '1 day'
        and tx_succeeded
    ) right join (
      select
        address as contract_address
      from monad.testnet.dim_contracts
    ) c using(contract_address)
  group by 1
  qualify row_number() over (order by txns desc) <= 1000
)

select
  contract_address
  , coalesce(
    case 
      when creator_address = '0xc816865f172d640d93712c68a7e1f83f3fa63235' then 'Kuru'
      when creator_address = '0x7fe0bce62b95b22eb6335b2dac3b4e5a2f6f034e' then 'Nad.fun'
      when creator_address = '0x321fb42877b7c61efe489505ab87bf1dff33f1e4' then 'Nad.fun'
      when creator_address = '0x60216fb3285595f4643f9f7cddab842e799bd642' then 'Nad.fun'
    end
    , l_c.name
    , d_c.name
    , symbol
    , 'NA'
    ) as contract_name
  , case when contract_name != 'NA' then contract_name else contract_address end as contract
  , coalesce(category, 'NA') as category
from smart_filter
left join monad.testnet.dim_contracts d_c on contract_address = d_c.address 
left join labelled_contracts l_c on contract_address = l_c.address
