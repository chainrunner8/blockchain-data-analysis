top_mdc_tokens = """
with

    kuru_tokens as (
        select address, created_block_timestamp from MONAD_CORE_DATA.TESTNET.DIM_CONTRACTS 
        where creator_address = '0xc816865f172d640d93712c68a7e1f83f3fa63235'
        and created_block_timestamp >= '2025-02-19 15:00:00'
    )

    , mob_da_clob_tokens as (
        select address from kuru_tokens where created_block_timestamp between '2025-05-05 15:00:00' and '2025-05-09 19:59:59'
    )
    -- mdc = mob da clob
    , mdc_fact_transactions as (
        select
            block_timestamp
            , from_address as trader
            , address as token_address
            , value
        from mob_da_clob_tokens
        left join (
            select
                block_timestamp
                , from_address
                , to_address
                , value
            from MONAD_CORE_DATA.TESTNET.FACT_TRANSACTIONS
            where block_timestamp between '2025-05-05 15:00:00' and '2025-05-09 19:59:59'
            and tx_succeeded
        ) on address = to_address
    )

select
token_address
, count(*) as txns
, sum(value) as volume
, count(distinct trader) as unique_traders
from mdc_fact_transactions
group by 1
order by 2 desc
limit 500;
"""

daily_traffic = """
with

    kuru_tokens as (
        select address, created_block_timestamp from MONAD_CORE_DATA.TESTNET.DIM_CONTRACTS 
        where creator_address = '0xc816865f172d640d93712c68a7e1f83f3fa63235'
        and created_block_timestamp >= '2025-02-19 15:00:00'
    )

    , kuru_fact_transactions as (
        select
            block_timestamp
            , from_address as trader
            , address as token_address
            , value
        from kuru_tokens
        left join (
            select
                block_timestamp
                , from_address
                , to_address
                , value
            from MONAD_CORE_DATA.TESTNET.FACT_TRANSACTIONS
            where block_timestamp >= '2025-02-19 15:00:00'
            and tx_succeeded
        ) on address = to_address
    )

select
    date_trunc(day, block_timestamp) as day
    , count(*) as txns
    , sum(value) as volume
    , count(distinct trader) as traders
    , count(distinct token_address) as tokens_traded
from kuru_fact_transactions
group by 1
order by 1
"""
