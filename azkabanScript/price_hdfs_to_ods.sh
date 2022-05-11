#!/bin/bash


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else 
   do_date=`date -d "-1 day" +%F`
fi 

echo ================== 日志日期为 $do_date ==================
sql_coin="
load data inpath '/user/upload/eth_json/coin_price/$do_date' into table eth.ods_coin_price partition(dt='$do_date');
"

sql_floor="
load data inpath '/user/upload/eth_json/floor_price/$do_date' into table eth.ods_floor_price partition(dt='$do_date');
"

hive -e "$sql_coin"
hive -e "$sql_floor"
