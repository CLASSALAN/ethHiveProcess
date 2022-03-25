set hive.exec.dynamic.partition.mode=nonstrict;
insert into eth.dws_erc20_stable_coins partition (dt)
select address contract_address, decimals, name, symbol, "2022-03-12" dt from dwd_eth_log_erctoken 
where name = "Alchemix USD"  or name = "Binance USD" or name = "Dai" or name = "STASIS EURS"
      or name = "Fei USD"  or name = "Frax" or name = "Gemini Dollar" or name = "HUSD"
      or name = "Liquity USD"  or name = "Magic Internet Money" or name = "mStable USD" or name = "Paxos Standard"
      or name = "Synthetix sUSD"  or name = "TrueUSD" or name = "USD Coin" or name = "Neutrino USD"
      or name = "Pax Dollar"  or name = "Tether" or name = "Wrapped UST Token"
group by address, name, decimals, symbol;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into eth.dws_erc20_token_balances partition (dt)
select
  eth.div_bignum(from_balance, power(10, decimals)) amount,
  from_balance amount_raw,
  `timestamp`,
  address token_address,
  symbol token_symbol,
  `from` wallet_address,
  "2022-03-12" dt
from eth.dwd_eth_log_erctoken where from_balance is not null and from_balance != "<nil>" and from_balance != "" 
                                and dt = "2022-03-12"and erc_type = "ERC20" and eth.parse_erc20_transfer(topics, data) is not null
group by from_balance, `timestamp`, address, symbol, `from`, decimals
union all
select
  eth.div_bignum(to_balance, power(10, decimals)) amount,
  to_balance amount_raw,
  `timestamp`,
  address token_address,
  symbol token_symbol,
  `to` wallet_address,
  "2022-03-12" dt
from eth.dwd_eth_log_erctoken where to_balance is not null and to_balance != "<nil>" and to_balance != "" 
                                and dt = "2022-03-12" and erc_type = "ERC20" and eth.parse_erc20_transfer(topics, data) is not null
group by to_balance, `timestamp`, address, symbol, `to`, decimals;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into eth.dws_erc20_token_balances_latest partition (dt)
select 
  tbl.amount_raw,
  tbl.`timestamp`,
  tbl.token_address,
  tbl.wallet_address,
  "2022-03-12" dt
from (select 
  tb.amount_raw,
  tb.`timestamp`,
  tb.token_address,
  tb.wallet_address
from eth.dws_erc20_token_balances tb join
(select 
  max(`timestamp`) max_ts,
  token_address,
  wallet_address
from eth.dws_erc20_token_balances group by token_address, wallet_address) tmp
on tb.`timestamp` = tmp.max_ts and tb.token_address = tmp.token_address and tb.wallet_address = tmp.wallet_address) tbl
group by 
  amount_raw,
  `timestamp`,
  token_address,
  wallet_address;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into eth.dws_erc20_tokens partition (dt)
select
  address contract_address,
  decimals,
  symbol,
  "2022-03-12" dt
from eth.dwd_eth_log_erctoken where erc_type = "ERC20" and symbol != "" and eth.parse_erc20_transfer(topics, data) is not null
group by
  address,
  decimals,
  symbol;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into eth.dws_erc20_weth_hourly_balance_changes partition (dt)
select 
  eth.sub_bignum("0",value) amount_raw,
  `timestamp` `hour`,
  address token_address,
  `from` wallet_address,
  "2022-03-12" dt
from eth.dwd_eth_log_erctoken where symbol = "WETH"
union all
select 
  value amount_raw,
  `timestamp` `hour`,
  address token_address,
  `to` wallet_address,
  "2022-03-12" dt
from eth.dwd_eth_log_erctoken where symbol = "WETH";

select ds, count(DISTINCT(txn_hash))
from (select date_format(from_utc_timestamp(cast(`timestamp` as bigint)*1000,"PST"),'yyyy-MM-dd') ds, txn_hash 
from dwd_eth_block_transaction where dt = "0000-00-03" 
and txn_to ='0x1c7e83f8c581a967940dbfa7984744646ae46b29' and txn_status = 1)tmp
group by ds;

------------------------------------------ Dune Test ----------------------------------------------
WITH transfers AS ( SELECT
   evt_tx_hash AS tx_hash,
     tr."from" AS address,
     -tr.value AS amount,contract_address

FROM erc20."ERC20_evt_Transfer" tr
WHERE contract_address =  '\x1c7e83f8c581a967940dbfa7984744646ae46b29'

UNION ALL

SELECT evt_tx_hash AS tx_hash,
           tr."to" AS address,
          tr.value AS amount,contract_address

FROM erc20."ERC20_evt_Transfer" tr
where contract_address = '\x1c7e83f8c581a967940dbfa7984744646ae46b29'),
transferAmounts AS (
                    SELECT address,
                    sum(amount)/1e18 as poolholdings FROM transfers

                    GROUP BY 1
                    ORDER BY 2 DESC)

SELECT COUNT(DISTINCT(address)) as holders
FROM transferAmounts
WHERE poolholdings > 0;
