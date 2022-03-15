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