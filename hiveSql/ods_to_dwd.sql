set hive.exec.dynamic.partition.mode=nonstrict;
insert into table eth.dwd_eth_block_transaction partition (dt)
select
    cast(`blockHeiget` as bigint) `block_height`,
    blockHash `block_hash`,
    parentHash `parent_hash`,
    sha3Uncles `sha3_uncles`,
    miner,
    stateRoot `state_root`,
    transactionsRoot `transactions_root`,
    receiptsRoot `receipts_root`,
    difficulty,
    cast(gasLimit as decimal(30,0)) block_gaslimit,
    cast(BlockGasUsed as decimal(30,0)) block_gasused,
    blockBaseFee block_basefee,
    extraData,
    mixDisgest mixhash,
    blockNonce block_nonce,
    cast(unclesNumber as int) block_unclesnumber,
    cast(`blockSize` as bigint) `block_size`,
    receivedFrom received_from,
    get_json_object(transactions, "$.txHash")  txn_hash,  
    get_json_object(transactions, "$.TxnFrom")  txn_from,  
    get_json_object(transactions, "$.txnTo")  txn_to,
    get_json_object(transactions, "$.txnContractAddress")  txn_contractaddress,  
    get_json_object(transactions, "$.size")  txn_size,  
    get_json_object(transactions, "$.txnType")  txn_type,  
    get_json_object(transactions, "$.txnStatus")  txn_status,  
    get_json_object(transactions, "$.chainId")  txn_chainid,  
    get_json_object(transactions, "$.txnInputData")  txn_inputdata,  
    get_json_object(transactions, "$.txnValue")  txn_value, 
    cast(get_json_object(transactions, "$.txnNonce") as decimal(30,0)) txn_nonce,
    get_json_object(transactions, "$.txnPostState")  txn_poststate,
    cast(get_json_object(transactions, "$.txnGasLimt") as decimal(30,0)) txn_gaslimit,    
    get_json_object(transactions, "$.txnGasPrice")  txn_gasprice,
    get_json_object(transactions, "$.txnGasTipCap")  txn_gastipcap,
    get_json_object(transactions, "$.txnGasFeeCap")  txn_gasfee,
    get_json_object(transactions, "$.txnMaxPriority")  txn_maxpriority,
    cast(get_json_object(transactions, "$.txGasUsed") as decimal(30,0)) txn_gasused, 
    cast(get_json_object(transactions, "$.txnCumulativeGasUsed") as decimal(30,0)) txn_cumulativegasused, 
    cast(get_json_object(transactions, "$.txnTransactionIndex") as bigint) txn_position, 
    `timeStamp`,
    "2022-03-10" dt
from
(select 
    get_json_object(line, "$.blockHeiget") as `blockHeiget`,
    get_json_object(line, "$.timeStamp") as `timeStamp`,
    nvl(get_json_object(line, "$.Transactions"),"[{}]")  as transaction,
    get_json_object(line, "$.parentHash") as parentHash,
    get_json_object(line, "$.sha3Uncles") as sha3Uncles,
    get_json_object(line, "$.stateRoot") as stateRoot,
    get_json_object(line, "$.transactionsRoot") as transactionsRoot,
    get_json_object(line, "$.receiptsRoot") as receiptsRoot,
    get_json_object(line, "$.difficulty") as difficulty,
    get_json_object(line, "$.gasLimit") as gasLimit,
    get_json_object(line, "$.BlockGasUsed") as BlockGasUsed,
    get_json_object(line, "$.blockBaseFee") as blockBaseFee,
    get_json_object(line, "$.extraData") as extraData,
    get_json_object(line, "$.blockHash") as blockHash,
    get_json_object(line, "$.mixDisgest") as mixDisgest,
    get_json_object(line, "$.blockNonce") as blockNonce,
    get_json_object(line, "$.blockSize") as `blockSize`,
    get_json_object(line, "$.receivedFrom") as `receivedFrom`,
    get_json_object(line, "$.unclesNumber") as `unclesNumber`,
    get_json_object(line, "$.receivedAt") as `receivedAt`,
    get_json_object(line, "$.miner") as `miner`
FROM eth.ods_eth_block where dt = "2022-03-10") tmp lateral view default.explode_json(transaction) explode_table as transactions;

set hive.exec.dynamic.partition.mode=nonstrict;
insert into table eth.dwd_eth_log_erctoken partition (dt)
select 
  address, 
  topics, 
  data, 
  transactionHash txn_hash, 
  cast(transactionIndex as bigint) txn_index, 
  blockHash block_hash, 
  cast(blockNumber as bigint) block_number, 
  cast(logIndex as bigint) log_index, 
  cast(removed as boolean) removed,
  GET_JSON_OBJECT(ercs, "$.creator") `creator`,
  GET_JSON_OBJECT(ercs, "$.ercType") `erc_type`,
  GET_JSON_OBJECT(ercs, "$.from") `from`, 
  GET_JSON_OBJECT(ercs, "$.to") `to`, 
  GET_JSON_OBJECT(ercs, "$.value") value, 
  GET_JSON_OBJECT(ercs, "$.fromBalance") from_balance, 
  GET_JSON_OBJECT(ercs, "$.toBalance") to_balance, 
  GET_JSON_OBJECT(ercs, "$.addressBalance") address_balance, 
  GET_JSON_OBJECT(ercs, "$.name") name,
  GET_JSON_OBJECT(ercs, "$.symbol") symbol,
  cast(GET_JSON_OBJECT(ercs, "$.decimals") as bigint) decimals,
  `timestamp`,
  "2022-03-10" dt
from(
select 
    GET_JSON_OBJECT(logs, "$.address") address,
    GET_JSON_OBJECT(logs, "$.topics") topics,
    GET_JSON_OBJECT(logs, "$.data") data,
    GET_JSON_OBJECT(logs, "$.blockNumber") blockNumber,
    GET_JSON_OBJECT(logs, "$.transactionHash") transactionHash,
    GET_JSON_OBJECT(logs, "$.transactionIndex") transactionIndex,
    GET_JSON_OBJECT(logs, "$.blockHash") blockHash,
    GET_JSON_OBJECT(logs, "$.logIndex") logIndex,
    GET_JSON_OBJECT(logs, "$.timeStamp") `timeStamp`,
    GET_JSON_OBJECT(logs, "$.removed") `removed`,
    nvl(GET_JSON_OBJECT(logs, "$.erctokens"), "[{}]") erctokens
from(
select default.explode_json(line) logs
from eth.ods_erc_token where dt = "2022-03-10") tmp1) tmp2 lateral view default.explode_json(erctokens) explode_table as ercs;

insert into table eth.dwd_eth_address_status partition (dt)
select
    address,
    balance,
    stotageKey storage_root,
    contractCode contract_code,
    cast(`nonce` as bigint) `nonce`,
    cast(`blockHeight` as bigint) `block_height`,
    `timestamp`,
    "2022-03-10" dt
from(
select 
    get_json_object(line, "$.address") as `address`,
    get_json_object(line, "$.balance") as `balance`,
    get_json_object(line, "$.stotageKey") as `stotageKey`,
    get_json_object(line, "$.contractCode") as `contractCode`,
    get_json_object(line, "$.nonce") as `nonce`,
    get_json_object(line, "$.blockHeight") as `blockHeight`,
    get_json_object(line, "$.timeStamp") as `timeStamp`
FROM (
select default.explode_json(line) line
from eth.ods_eth_address where dt = "2022-03-10")tmp1) tmp2;

insert into table eth.dwd_coin_price partition (dt)
select 
    get_json_object(line, "$.get_time") as `get_time`,
    get_json_object(line, "$.web_time") as `web_time`,
    get_json_object(line, "$.get_time_day") as `get_time_day`,
    cast(get_json_object(line, "$.get_time_hour") as int) as `get_time_hour`,
    get_json_object(line, "$.token_symbol") as `token_symbol`,
    cast(replace(get_json_object(line, "$.coin2rmb"), ",", "") as float) as `coin2rmb`,
    cast(replace(get_json_object(line, "$.coin2usd"), ",", "") as float) as `coin2usd`,
    "2022-03-10" dt
FROM eth.ods_coin_price;