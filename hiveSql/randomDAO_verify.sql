SELECT `date`, sum(users) OVER (
                              ORDER BY `date` ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS total_users
FROM
  (SELECT `date`, count(`USER`) AS users
   FROM
     (SELECT min(`date`) AS `date`,
             account AS `USER`
      FROM
        (select min(`date`) `date`, account from (SELECT SUBSTR(evt_block_time, 1, 10) AS `date`,
                t.`from` AS account
         FROM dws_erc20_evt_transfer t
         WHERE t.contract_address = '0x1c7e83f8c581a967940dbfa7984744646ae46b29')tmp1
         GROUP BY account
         UNION all
         select min(`date`) `date`, account from (SELECT SUBSTR(evt_block_time, 1, 10) AS `date`,
                      t.`to` AS account
         FROM dws_erc20_evt_transfer t
         WHERE t.contract_address = '0x1c7e83f8c581a967940dbfa7984744646ae46b29')tmp2
         GROUP BY account) AS a
      GROUP BY account) AS b
   GROUP BY `date`
   ORDER BY `date`) AS c;
 
SELECT `date`, sum(users) OVER (
                              ORDER BY `date` ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS total_users
FROM
  (SELECT `date`, count(`USER`) AS users
   FROM
     (SELECT min(`date`) AS `date`,
             account AS `USER`
      FROM(
select min(`date`) `date`, account from (SELECT SUBSTR(evt_block_time, 1, 16) AS `date`,
                t.`from` AS account
         FROM dws_erc20_evt_transfer t
         WHERE t.contract_address = '0x1c7e83f8c581a967940dbfa7984744646ae46b29'
         and SUBSTR(evt_block_time, 1, 13) = '2022-02-06 13')tmp1
         GROUP BY account
         UNION all
         select min(`date`) `date`, account from (SELECT SUBSTR(evt_block_time, 1, 16) AS `date`,
                      t.`to` AS account
         FROM dws_erc20_evt_transfer t
         WHERE t.contract_address = '0x1c7e83f8c581a967940dbfa7984744646ae46b29'
         and SUBSTR(evt_block_time, 1, 13) = '2022-02-06 13')tmp2
         GROUP BY account) as a
       group by account) AS b
   GROUP BY `date`
   ORDER BY `date`) AS c;

select evt_block_time, `from`, `to`, value 
from dws_erc20_evt_transfer t
         WHERE t.contract_address = '0x1c7e83f8c581a967940dbfa7984744646ae46b29'
         and SUBSTR(evt_block_time, 1, 16) = '2022-02-06 13:30';

select * from dws_erc20_evt_transfer
where evt_tx_hash = '0x03175be98e30bcd5be84680ab649a27fe520210acbd21af57561ec3ff1765416';

select DISTINCT txn_hash, block_number 
from dwd_eth_log_erctoken 
where block_number = 14152801;

select * from dws_erc20_evt_transfer where evt_block_number = 14130000;

select *
from dwd_eth_log_erctoken 
where txn_hash = '0x03175be98e30bcd5be84680ab649a27fe520210acbd21af57561ec3ff1765416';
