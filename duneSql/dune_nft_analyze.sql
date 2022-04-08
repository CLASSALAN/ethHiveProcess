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
