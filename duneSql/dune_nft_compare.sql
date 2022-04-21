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

-------- addresses that profit from multiple nft projects --------
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
from nft."trades_v2_beta" where nft_contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
),
buy_table_a as (
select
    block_time,
    nft_token_id,
    buyer,
    eth_amount,
    usd_amount
from nft."trades_v2_beta" where nft_contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
),
profit_table_a as(
select
    st.nft_project_name,
    st.seller as address,
    sum(bt.eth_amount) as buy_price,
    sum(st.eth_amount) as sell_price,
    sum(st.eth_income) - sum(bt.eth_amount) as eth_profit,
    sum(st.usd_income) - sum(bt.usd_amount) as usd_profit
from sell_table_a st join buy_table_a bt
on st.seller = bt.buyer and st.nft_token_id = bt.nft_token_id
group by st.nft_project_name, st.seller
),
sell_table_b as(
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
from nft."trades_v2_beta" where nft_contract_address = '\x60E4d786628Fea6478F785A6d7e704777c86a7c6'
),
buy_table_b as (
select
    block_time,
    nft_token_id,
    buyer,
    eth_amount,
    usd_amount
from nft."trades_v2_beta" where nft_contract_address = '\x60E4d786628Fea6478F785A6d7e704777c86a7c6'
),
profit_table_b as(
select
    st.nft_project_name,
    st.seller as address,
    sum(bt.eth_amount) as buy_price,
    sum(st.eth_amount) as sell_price,
    sum(st.eth_income) - sum(bt.eth_amount) as eth_profit,
    sum(st.usd_income) - sum(bt.usd_amount) as usd_profit
from sell_table_b st join buy_table_b bt
on st.seller = bt.buyer and st.nft_token_id = bt.nft_token_id
group by st.nft_project_name, st.seller
)
select
    pa.address as address,
    pa.nft_project_name as nft_a_name,
    pa.eth_profit as nft_a_eth_profit,
    pa.usd_profit as nft_a_usd_profit,
    pb.nft_project_name as nft_b_name,
    pb.eth_profit as nft_b_eth_profit,
    pb.usd_profit as nft_b_usd_profit
from profit_table_a pa join profit_table_b pb
on pa.address = pb.address
where pa.eth_profit > 0 and pa.usd_profit > 0 and pb.eth_profit > 0 and pb.usd_profit > 0;
