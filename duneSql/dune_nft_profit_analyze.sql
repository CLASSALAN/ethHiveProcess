with seller_sum as(
select
    "seller",
    sum("eth_amount") as sum_eth_amount, --ETH数额
    sum("usd_amount") as sum_usd_amount, --USD对应价值
    sum("original_royalty_fees") as sum_original_royalty_fees, --royalty fee ETH数额
    sum("usd_royalty_fees") as sum_usd_royalty_fees, --royalty fee USD对应价值
    sum("original_platform_fees") as sum_original_platform_fees, --平台收取ETH数额
    sum("usd_platform_fees") as  sum_usd_platform_fees --平台收取USD对应价值
from nft."trades_v2_beta"
where erc_standard like '%721' and nft_contract_address = '\xED5AF388653567Af2F388E6224dC7C4b3241C544'
group by "seller"
),
income_total as (
select 
    "seller",
    sum_eth_amount - sum_original_royalty_fees - sum_original_platform_fees as income_eth_sum,
    sum_usd_amount - sum_usd_royalty_fees - sum_usd_platform_fees as income_usd_sum,
    sum_original_royalty_fees, --royalty fee ETH数额
    sum_usd_royalty_fees, --royalty fee USD对应价值
    sum_original_platform_fees, --平台收取ETH数额
    sum_usd_platform_fees --平台收取USD对应价值
from seller_sum
),
outcome_total as(
select
    "nft_project_name",
    "nft_contract_address",
    "buyer",
    sum("eth_amount") as outcome_eth_sum, --购买所花费的ETH数量
    sum("usd_amount") as outcome_usd_sum --所对应的USD价值
from nft."trades_v2_beta"
where erc_standard like '%721' and nft_contract_address = '\xED5AF388653567Af2F388E6224dC7C4b3241C544'
group by "nft_project_name", "nft_contract_address", "buyer"
)
select 
    "nft_project_name",
    "nft_contract_address",
    it.seller as trader_address,
    coalesce(it.income_eth_sum, 0) - coalesce(ot.outcome_eth_sum, 0) as eth_total_profit,
    coalesce(it.income_usd_sum, 0) - coalesce(ot.outcome_usd_sum, 0) as usd_total_profit,
    coalesce(sum_original_royalty_fees, 0) as total_eth_royalty_fees, --royalty fee ETH数额
    coalesce(sum_usd_royalty_fees, 0) as total_usd_royalty_fees, --royalty fee USD对应价值
    coalesce(sum_original_platform_fees, 0) as total_original_platform_fees, --平台收取ETH数额
    coalesce(sum_usd_platform_fees, 0) as total_usd_platform_fees--平台收取USD对应价值
from income_total it full join outcome_total ot on it.seller = ot.buyer
where it.seller is not null and ot.buyer is not null;

---------- profit with mint cost ----------
with seller_sum as(
select
    "seller",
    sum("eth_amount") as sum_eth_amount, --ETH数额
    sum("usd_amount") as sum_usd_amount, --USD对应价值
    sum("original_royalty_fees") as sum_original_royalty_fees, --royalty fee ETH数额
    sum("usd_royalty_fees") as sum_usd_royalty_fees, --royalty fee USD对应价值
    sum("original_platform_fees") as sum_original_platform_fees, --平台收取ETH数额
    sum("usd_platform_fees") as  sum_usd_platform_fees --平台收取USD对应价值
from nft."trades_v2_beta"
where erc_standard like '%721' and nft_contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
group by "seller"
),
income_total as (
select
    "seller",
    sum_eth_amount - sum_original_royalty_fees - sum_original_platform_fees as income_eth_sum,
    sum_usd_amount - sum_usd_royalty_fees - sum_usd_platform_fees as income_usd_sum,
    sum_original_royalty_fees, --royalty fee ETH数额
    sum_usd_royalty_fees, --royalty fee USD对应价值
    sum_original_platform_fees, --平台收取ETH数额
    sum_usd_platform_fees --平台收取USD对应价值
from seller_sum
),
outcome_total as(
select
    "nft_project_name",
    "nft_contract_address",
    "buyer",
    sum("eth_amount") as outcome_eth_sum, --购买所花费的ETH数量
    sum("usd_amount") as outcome_usd_sum --所对应的USD价值
from nft."trades_v2_beta"
where erc_standard like '%721' and nft_contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
group by "nft_project_name", "nft_contract_address", "buyer"
),
profit as (select
    "nft_project_name",
    "nft_contract_address",
    it.seller as trader_address,
    coalesce(it.income_eth_sum, 0) - coalesce(ot.outcome_eth_sum, 0) as eth_total_profit,
    coalesce(it.income_usd_sum, 0) - coalesce(ot.outcome_usd_sum, 0) as usd_total_profit,
    coalesce(sum_original_royalty_fees, 0) as total_eth_royalty_fees, --royalty fee ETH数额
    coalesce(sum_usd_royalty_fees, 0) as total_usd_royalty_fees, --royalty fee USD对应价值
    coalesce(sum_original_platform_fees, 0) as total_original_platform_fees, --平台收取ETH数额
    coalesce(sum_usd_platform_fees, 0) as total_usd_platform_fees--平台收取USD对应价值
from income_total it full join outcome_total ot on it.seller = ot.buyer
where it.seller is not null and ot.buyer is not null
),
mint_transactions as (
select
    "to" as mint_address,
    evt_tx_hash
from erc721."ERC721_evt_Transfer"
where contract_address = '\xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D' and "from" = '\x0000000000000000000000000000000000000000'
group by 1, 2
),
mint_cost as (select
    mint_address,
    sum((value + et.gas_price*et.gas_used)/10^18) as eth_cost,
    sum((value + et.gas_price*et.gas_used)/10^18*peth.price) as usd_cost
from mint_transactions mt join ethereum."transactions" et on mt.mint_address = et."from" and mt.evt_tx_hash = et.hash
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', et.block_time) = peth.minute
group by mint_address
)
select
    "nft_project_name",
    "nft_contract_address",
    trader_address,
    eth_total_profit - coalesce(mc.eth_cost, 0) as eth_total_profit,
    usd_total_profit - coalesce(mc.usd_cost, 0) as usd_total_profit,
    total_eth_royalty_fees, --royalty fee ETH数额
    total_usd_royalty_fees, --royalty fee USD对应价值
    total_original_platform_fees, --平台收取ETH数额
    total_usd_platform_fees--平台收取USD对应价值
from profit pf left join mint_cost mc on pf.trader_address = mc.mint_address;
