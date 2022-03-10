load data inpath '/user/upload/eth_json/eth_block_data/2022-03-10' into table eth.ods_eth_block partition(dt='2022-03-10');

load data inpath '/user/upload/eth_json/erc_token_data/2022-03-10' into table eth.ods_erc_token partition(dt='2022-03-10');

load data inpath '/user/upload/eth_json/eth_address_data/2022-03-10' into table eth.ods_eth_address partition(dt='2022-03-10');

load data inpath '/user/upload/eth_json/coin_price/2022-03-10' into table eth.ods_coin_price partition(dt='2022-03-10');

