WITH swaps AS (
SELECT
  block_timestamp,
  tx_hash,
  contract_address,
  event_inputs :amount0In:: INTEGER AS amount0in,
  event_inputs :amount0Out:: INTEGER AS amount0out,
  event_inputs :amount1In:: INTEGER AS amount1in,
  event_inputs :amount1Out:: INTEGER AS amount1out
FROM ethereum.core.fact_event_logs
WHERE contract_address = lower('0xCEfF51756c56CeFFCA006cD410B03FFC46dd3a58')
 AND DATE(block_timestamp) BETWEEN '2022-04-01' AND '2022-04-15'
 AND event_name = 'Swap'
--ORDER BY block_timestamp DESC  
),
pool_details AS (
SELECT
  pool_name,
  pool_address,
  token0_symbol,
  token0_decimals,
  token1_symbol,
  token1_decimals
FROM ethereum.core.dim_dex_liquidity_pools
WHERE pool_address = lower('0xCEfF51756c56CeFFCA006cD410B03FFC46dd3a58')  
),
swap_pool_details AS (
SELECT
  block_timestamp,
  tx_hash,
  contract_address,
  pool_name,
  pool_address,
  token0_symbol,
  token0_decimals,
  amount0in,
  amount0out,
  token1_symbol,
  token1_decimals,
  amount1in,
  amount1out
FROM swaps
LEFT JOIN pool_details
ON contract_address = pool_address
),
final_outlook AS (
SELECT
  block_timestamp,
  tx_hash,
  contract_address,
  pool_name,
  pool_address,
  token0_symbol,
  amount0in / pow(10,token0_decimals) AS amount0in_adj,
  amount0out / pow(10,token0_decimals) AS amount0out_adj,
  token1_symbol,
  amount1in / pow(10,token1_decimals) AS amount1in_adj,
  amount1out / pow(10,token1_decimals) AS amount1out_adj
FROM swap_pool_details  
)
SELECT
  date_trunc('day', block_timestamp) AS DATE,
  count(tx_hash) AS SWAP_COUNT,
  sum(amount0in_adj) + sum(amount0out_adj) AS WBTC_VOL,
  sum(amount1in_adj) + sum(amount1out_adj) AS ETH_VOL
FROM final_outlook
GROUP BY DATE
ORDER BY DATE
