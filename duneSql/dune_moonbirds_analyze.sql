-------- MoonBirds and BAYC mutual address --------
with nfta_agg as
    (with transfers as
        (
        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'mint' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x23581767a106ae21c074b2276D25e5C3e136a68b'
        and "from" = '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'gain' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x23581767a106ae21c074b2276D25e5C3e136a68b'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'lose' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x23581767a106ae21c074b2276D25e5C3e136a68b'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all
        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'burn' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\x23581767a106ae21c074b2276D25e5C3e136a68b'
        and "to" = '\x0000000000000000000000000000000000000000')
        )

    select wallet,
           sum(value) as num_a
    from transfers
    group by wallet
    order by num_a desc
    ),
nfta_wallets as
(SELECT nfta_agg.wallet as a_wallets, num_a as num
FROM nfta_agg
where num_a > 0),

nftb_agg as
    (with transfers as
        (
        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'mint' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
        and "from" = '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "to" as wallet,
        "tokenId" as token_id,
        'gain' as action,
        1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all

        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'lose' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
        and "from" != '\x0000000000000000000000000000000000000000')

        union all
        (SELECT
        "from" as wallet,
        "tokenId" as token_id,
        'burn' as action,
        -1 as value
        FROM erc721."ERC721_evt_Transfer"
        where contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
        and "to" = '\x0000000000000000000000000000000000000000')
        )
    select wallet,
           sum(value) as num_a
    from transfers
    group by wallet
    order by num_a desc
    ),
nftb_wallets as
(SELECT nftb_agg.wallet as b_wallets, num_a as num
FROM nftb_agg
where num_a > 0)

select b_wallets as wallets, aw.num as nfta_num ,bw.num as nftb_num
from nfta_wallets aw join nftb_wallets bw
on aw.a_wallets = bw.b_wallets;

-------- MoonBirds profit address analyze --------
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
),
buy_table_a as (
select
    block_time,
    nft_token_id,
    buyer,
    eth_amount,
    usd_amount
from nft."trades_v2_beta" where nft_contract_address = '\x23581767a106ae21c074b2276D25e5C3e136a68b' and platform = 'OpenSea'
)
select
    st.nft_project_name,
    st.seller as address,
    st.nft_token_id,
    bt.block_time as "Buy Time",
    st.block_time as "Sell Time",
    bt.eth_amount as buy_price,
    st.eth_amount as sell_price,
    st.eth_income - bt.eth_amount as eth_profit,
    st.usd_income - bt.usd_amount as usd_profit
from sell_table_a st join buy_table_a bt
on st.seller = bt.buyer and st.nft_token_id = bt.nft_token_id;

