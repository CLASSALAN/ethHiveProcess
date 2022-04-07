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
where num_a > 0 
