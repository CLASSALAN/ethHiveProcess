create external table eth.ods_eth_block (`line` string)
partitioned by (`dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS SEQUENCEFILE
LOCATION
  'hdfs://hdfscluster/warehouse/eth/ods/ods_eth_block';
 
create external table eth.ods_erc_token (`line` string)
partitioned by (`dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS SEQUENCEFILE
LOCATION
  'hdfs://hdfscluster/warehouse/eth/ods/ods_erc_token';
 
create external table eth.ods_eth_address (`line` string)
partitioned by (`dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS SEQUENCEFILE
LOCATION
  'hdfs://hdfscluster/warehouse/eth/ods/ods_eth_address';

create external table ods_coin_price (`line` string)
partitioned by (`dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS SEQUENCEFILE
LOCATION
  'hdfs://hdfscluster/warehouse/eth/ods/ods_coin_price';
