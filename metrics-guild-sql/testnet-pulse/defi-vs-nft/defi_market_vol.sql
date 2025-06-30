with 

contracts as (
 select * from $query('2bc7dc97-d2ae-4744-bc3e-20fc230d46e1')  -- nadscope/contract-ldb/trending_contracts.sql
)

, defi_contracts as (
select *
from contracts
where category = 'DeFi'
)

select
  date_trunc(day, block_timestamp) as day
  , sum(value_precise) as volume
from monad.testnet.fact_transactions 
right join defi_contracts on to_address = contract_address
where 1=1
  and (block_timestamp between '2025-05-17 00:00:00' and (date_trunc(day, sysdate()) - interval '1 second') )
  and tx_succeeded
group by 1
order by 1
