#!/bin/bash


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else 
   do_date=`date -d "-1 day" +%F`
fi 

dwd_coin_price='
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table eth.dwd_coin_price partition (dt)
select 
    get_json_object(line, "$.get_time") as `get_time`,
    get_json_object(line, "$.web_time") as `web_time`,
    get_json_object(line, "$.get_time_day") as `get_time_day`,
    cast(get_json_object(line, "$.get_time_hour") as int) as `get_time_hour`,
    get_json_object(line, "$.token_symbol") as `token_symbol`,
    cast(replace(get_json_object(line, "$.coin2rmb"), ",", "") as float) as `coin2rmb`,
    cast(replace(get_json_object(line, "$.coin2usd"), ",", "") as float) as `coin2usd`,
    "'$do_date'" dt
FROM eth.ods_coin_price where dt = "'$do_date'";
'

dwd_floor_price='
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table eth.dwd_floor_price partition (dt)
select 
  get_json_object(line, "$.nft_createdata") as `nft_createdata`, 
  get_json_object(line, "$.nft_name") as `nft_name`, 
  get_json_object(line, "$.nft_slug") as `nft_slug`, 
  get_json_object(line, "$.nft_nativePaymentAsset") as `nft_nativePaymentAsset`, 
  cast(get_json_object(line, "$.nft_floorPrice") as double) as `nft_floorPrice`, 
  cast(get_json_object(line, "$.nft_numOwners") as double) as `nft_numOwners`, 
  cast(get_json_object(line, "$.nft_totalSupply") as double) as `nft_totalSupply`, 
  cast(get_json_object(line, "$.nft_oneDayChange") as double) as `nft_oneDayChange`, 
  cast(get_json_object(line, "$.nft_oneDayVolume") as double) as `nft_oneDayVolume`, 
  cast(get_json_object(line, "$.nft_thirtyDayChange") as double) as `nft_thirtyDayChange`, 
  cast(get_json_object(line, "$.nft_thirtyDayVolume") as double) as `nft_thirtyDayVolume`, 
  cast(get_json_object(line, "$.nft_sevenDayVolume") as double) as `nft_sevenDayVolume`, 
  cast(get_json_object(line, "$.nft_sevenDayChange") as double) as `nft_sevenDayChange`, 
  cast(get_json_object(line, "$.nft_totalVolume") as double) as `nft_totalVolume`, 
  get_json_object(line, "$.data_collection_time") as `data_collection_time`, 
  get_json_object(line, "$.nft_hash_addr") as `nft_hash_addr`, 
  "'$do_date'" dt
FROM eth.ods_floor_price where dt = "'$do_date'";
'

hive -e "$dwd_coin_price"
hive -e "$dwd_floor_price"
