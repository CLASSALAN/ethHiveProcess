set hive.exec.dynamic.partition.mode=nonstrict;
insert into eth.dws_erc20_stable_coins partition (dt)
select address contract_address, decimals, name, symbol, "2022-03-12" dt from dwd_eth_log_erctoken 
where symbol = "alUSD"  or symbol = "BUSD" or symbol = "DAI" or symbol = "EURS"
      or symbol = "FEI"  or symbol = "FRAX" or symbol = "GUSD" or symbol = "HUSD"
      or symbol = "LUSD"  or symbol = "MIM" or symbol = "MUSD" or symbol = "PAX"
      or symbol = "sUSD"  or symbol = "TUSD" or symbol = "USDC" or symbol = "USDN"
      or symbol = "USDP"  or symbol = "USDT" or symbol = "UST"
group by address, symbol, decimals, name;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into eth.dws_erc20_token_balances partition (dt)
select
  if(from_balance = "<nil>", "0", eth.div_bignum(from_balance, power(10, decimals)) amount,
  from_balance amount_raw,
  `timestamp`,
  address token_address,
  symbol token_symbol,
  `from` wallet_address,
  "2022-03-12" dt
from eth.dwd_eth_log_erctoken where `from` != "0x0000000000000000000000000000000000000000" 
                               and symbol is not null and dt = "2022-03-12"and erc_type = "ERC20"
group by from_balance, `timestamp`, address, symbol, `from`, dt
union all
select
  if(to_balance = "<nil>", "0", eth.div_bignum(to_balance, power(10, decimals))) amount,
  to_balance amount_raw,
  `timestamp`,
  address token_address,
  symbol token_symbol,
  `to` wallet_address,
  "2022-03-12" dt
from eth.dwd_eth_log_erctoken where `to` != "0x0000000000000000000000000000000000000000" 
                               and symbol is not null and dt = "2022-03-12" and erc_type = "ERC20"
group by to_balance, `timestamp`, address, symbol, `to`, decimals, dt;
