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


