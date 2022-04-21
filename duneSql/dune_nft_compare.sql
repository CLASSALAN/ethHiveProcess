-------- compare traders of different nft projects --------
-- analyze addresses that gain profit in the nft project
with sell_table as(
select
    block_time,
    platform,
    nft_contract_address,
    nft_project_name,
    nft_token_id,
    seller,
    eth_amount,
    usd_amount,
    original_royalty_fees,
    usd_royalty_fees,
    original_platform_fees,
    usd_platform_fees,
    eth_amount - original_royalty_fees - original_platform_fees as eth_income,
    usd_amount - usd_royalty_fees - usd_platform_fees as usd_income
from nft."trades_v2_beta" where nft_contract_address = '\xed5af388653567af2f388e6224dc7c4b3241c544'
),
buy_table as (
select
    block_time,
    nft_token_id,
    buyer,
    eth_amount,
    usd_amount
from nft."trades_v2_beta" where nft_contract_address = '\xed5af388653567af2f388e6224dc7c4b3241c544'
)
select
    st.nft_project_name,
    st.nft_token_id,
    st.platform,
    st.seller as address,
    bt.eth_amount as buy_price,
    st.eth_amount as sell_price,
    bt.block_time as buy_time,
    st.block_time as sell_time,
    st.eth_income - bt.eth_amount as eth_profit,
    st.usd_income - bt.usd_amount as usd_profit
from sell_table st join buy_table bt
on st.seller = bt.buyer and st.nft_token_id = bt.nft_token_id;
