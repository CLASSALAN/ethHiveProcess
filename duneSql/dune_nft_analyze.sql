with a_agg as 
    (with transfers as 
        (
        (SELECT 
        "to" as wallet,
        "tokenId" as token_id,
        'mint' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4b61413d4392c806e6d0ff5ee91e6073c21d6430'
        and "from" = '\x0000000000000000000000000000000000000000')
        
        union all
        
        (SELECT 
        "to" as wallet,
        "tokenId" as token_id,
        'gain' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4b61413d4392c806e6d0ff5ee91e6073c21d6430'
        and "from" != '\x0000000000000000000000000000000000000000')
        
        union all 
        
        (SELECT 
        "from" as wallet,
        "tokenId" as token_id,
        'lose' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4b61413d4392c806e6d0ff5ee91e6073c21d6430'
        and "from" != '\x0000000000000000000000000000000000000000')
        
        union all
        (SELECT 
        "from" as wallet,
        "tokenId" as token_id,
        'burn' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4b61413d4392c806e6d0ff5ee91e6073c21d6430'
        and "to" = '\x0000000000000000000000000000000000000000')
        )
        
    select wallet,
           sum(value) as num_a
    from transfers
    group by wallet
    order by num_a desc
    )

SELECT count(a_agg.wallet) as wallets
FROM a_agg
where num_a > 0;

------azuki vs muri holders------
with a_agg as
    (with transfers as
        (
        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'mint' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xED5AF388653567Af2F388E6224dC7C4b3241C544'
        and "from" = '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'gain' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xED5AF388653567Af2F388E6224dC7C4b3241C544'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'lose' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xED5AF388653567Af2F388E6224dC7C4b3241C544'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all
        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'burn' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xED5AF388653567Af2F388E6224dC7C4b3241C544'
        and "to" = '\x0000000000000000000000000000000000000000')
        )

    select wallet,
           sum(value) as num_a
    from transfers
    group by wallet
    order by num_a desc
    ),
azuki_wallets as
(SELECT a_agg.wallet as a_wallets
FROM a_agg
where num_a > 0),

m_agg as
    (with transfers as
        (
        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'mint' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4B61413D4392c806E6d0fF5Ee91E6073C21d6430'
        and "from" = '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'gain' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4B61413D4392c806E6d0fF5Ee91E6073C21d6430'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'lose' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4B61413D4392c806E6d0fF5Ee91E6073C21d6430'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all
        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'burn' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x4B61413D4392c806E6d0fF5Ee91E6073C21d6430'
        and "to" = '\x0000000000000000000000000000000000000000')
        )
    select wallet,
           sum(value) as num_a
    from transfers
    group by wallet
    order by num_a desc
    ),
    muri_wallets as
(SELECT m_agg.wallet as m_wallets, num_a as num
FROM m_agg
where num_a > 0)

select m_wallets, mw.num
from azuki_wallets aw join muri_wallets mw
on aw.a_wallets = mw.m_wallets;

---------------- nft holders profit analyze ----------------
with et as (
select
    "from",
    "to",
    "tokenId",
    "contract_address",
    "evt_tx_hash",
    "evt_index",
    "evt_block_time",
    "evt_block_number"
from erc721."ERC721_evt_Transfer"
),
with nt as (
select
    "block_time",
    "nft_project_name",
    "nft_token_id",
    "erc_standard",
    "platform",
    "platform_version",
    "trade_type",
    "number_of_items",
    "category",
    "evt_type",
    "usd_amount",
    "seller",
    "buyer",
    "original_amount",
    "original_amount_raw",
    "original_currency",
    "original_currency_contract",
    "currency_contract",
    "nft_contract_address",
    "exchange_contract_address",
    "tx_hash",
    "block_number",
    "nft_token_ids_array",
    "senders_array",
    "recipients_array",
    "erc_types_array",
    "nft_contract_addresses_array",
    "erc_values_array",
    "tx_from",
    "tx_to",
    "trace_address",
    "evt_index",
    "trade_id"
from nft.trades
where erc_standard like '%721'
)
select
    *
from et join nt on et."evt_tx_hash" = nt."tx_hash";

-------- trades that sell within 24 hours after buying --------
with sell_table_a as(
select
    block_time,
    platform,
    nft_contract_address,
    nft_project_name,
    nft_token_id,
    seller,
    eth_amount,
    usd_amount,
    coalesce(original_royalty_fees, 0),
    coalesce(usd_royalty_fees, 0),
    coalesce(original_platform_fees, 0),
    coalesce(usd_platform_fees, 0),
    eth_amount - coalesce(original_royalty_fees, 0) - coalesce(original_platform_fees, 0) as eth_income,
    usd_amount - coalesce(usd_royalty_fees, 0) - coalesce(usd_platform_fees, 0) as usd_income
from nft."trades_v2_beta" where nft_contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D' and platform = 'OpenSea'
    and eth_amount is not null and usd_amount is not null and (original_currency = 'ETH' OR original_currency = 'WETH')
),
buy_table_a as (
select
    block_time,
    nft_token_id,
    buyer,
    eth_amount,
    usd_amount
from nft."trades_v2_beta" where nft_contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D' and platform = 'OpenSea'
    and eth_amount is not null and usd_amount is not null and (original_currency = 'ETH' OR original_currency = 'WETH')
group by block_time, nft_token_id, buyer, eth_amount, usd_amount
)
select
    st.nft_project_name,
    st.seller as address,
    st.nft_token_id,
    bt.block_time as "Buy Time",
    st.block_time as "Sell Time",
    EXTRACT(EPOCH FROM (st.block_time - bt.block_time))/60/60 as "Time Difference", --时间差距（小时）
    bt.eth_amount as buy_price,
    st.eth_amount as sell_price,
    st.eth_income - bt.eth_amount as eth_profit,
    st.usd_income - bt.usd_amount as usd_profit
from sell_table_a st join buy_table_a bt
on st.seller = bt.buyer and st.nft_token_id = bt.nft_token_id
WHERE EXTRACT(EPOCH FROM (st.block_time - bt.block_time))/60/60 > 0
    and EXTRACT(EPOCH FROM (st.block_time - bt.block_time))/60/60 < 24;

-------- paper hands trades ratio --------
with sell_table_a as(
select 
    block_time,
    platform,
    nft_contract_address,
    nft_project_name,
    nft_token_id,
    seller, 
    eth_amount,
    usd_amount,
    coalesce(original_royalty_fees, 0), 
    coalesce(usd_royalty_fees, 0),
    coalesce(original_platform_fees, 0),
    coalesce(usd_platform_fees, 0),
    eth_amount - coalesce(original_royalty_fees, 0) - coalesce(original_platform_fees, 0) as eth_income,
    usd_amount - coalesce(usd_royalty_fees, 0) - coalesce(usd_platform_fees, 0) as usd_income
from nft."trades_v2_beta" where nft_contract_address = '\x23581767a106ae21c074b2276D25e5C3e136a68b' and platform = 'OpenSea'
    and eth_amount is not null and usd_amount is not null and (original_currency = 'ETH' OR original_currency = 'WETH')
),
buy_table_a as (
select 
    block_time,
    nft_token_id,
    buyer,
    eth_amount,
    usd_amount
from nft."trades_v2_beta" where nft_contract_address = '\x23581767a106ae21c074b2276D25e5C3e136a68b' and platform = 'OpenSea'
    and eth_amount is not null and usd_amount is not null and (original_currency = 'ETH' OR original_currency = 'WETH')
group by block_time, nft_token_id, buyer, eth_amount, usd_amount
),
paper_hands_table as (select
    st.nft_project_name,
    st.seller as address,
    st.nft_token_id,
    bt.block_time as "Buy Time",
    st.block_time as "Sell Time",
    EXTRACT(EPOCH FROM (st.block_time - bt.block_time))/60/60 as time_difference, --时间差距（小时）
    case when EXTRACT(EPOCH FROM (st.block_time - bt.block_time))/60/60 < 24 then 1
        else 0 end as paper_hands_count,
    bt.eth_amount as buy_price,
    st.eth_amount as sell_price,
    st.eth_income - bt.eth_amount as eth_profit,
    st.usd_income - bt.usd_amount as usd_profit
from sell_table_a st join buy_table_a bt
on st.seller = bt.buyer and st.nft_token_id = bt.nft_token_id
WHERE EXTRACT(EPOCH FROM (st.block_time - bt.block_time))/60/60 > 0)
select 
    nft_project_name,
    address, 
    count(*) as trades_count,
    sum(paper_hands_count) as paper_hands_count,
    cast(sum(paper_hands_count) as float)/count(*) as paper_hands_ratio
from paper_hands_table
group by nft_project_name, address;

