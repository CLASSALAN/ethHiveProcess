-------- analyze floor price and avg price for nft projects --------
SELECT 
    date_trunc('day', block_time) as "Day",
    sum(original_amount) as "Volume Ξ",
    percentile_cont(.1) within group (order by original_amount) as "Floor Price Ξ",
    min(original_amount) as min_amount
FROM nft.trades nftt
WHERE nftt."original_currency" IN ('ETH', 'WETH') 
    AND number_of_items = 1 AND original_amount > 0.01
    AND nft_contract_address = '\xed5af388653567af2f388e6224dc7c4b3241c544'
group by "Day"
order by "Day" DESC;
