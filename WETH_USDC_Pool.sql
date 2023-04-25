-- get details for relevant pool
WITH pools AS (
  SELECT
  pool_name,
  pool_address,
  token0,
  token1
  FROM ethereum.core.dim_dex_liquidity_pools
  WHERE pool_address = lower('0x397FF1542f962076d0BFE58eA045FfA2d347ACa0')
  ),
  -- get details for tokens in relevant pool
  decimals AS (
  SELECT
	address,
	symbol,
	decimals
  FROM ethereum.core.dim_contracts
  WHERE address = (
  	SELECT
  		Lower(token1)
  	FROM pools
	)
	OR address = (
  	SELECT
  		Lower(token0)
  	FROM pools
    )
  ),
-- aggregate pool and token details
  pool_token_details AS (
  SELECT
  pool_name,
  pool_address,
  token0,
  token1,
  token0.symbol AS token0symbol,
  token1.symbol AS token1symbol,
  token0.decimals AS token0decimals,
  token1.decimals AS token1decimals
FROM 
	pools
	LEFT JOIN decimals AS token0
	ON token0.address = token0
	LEFT JOIN decimals AS token1
	ON token1.address = token1
),
-- find swaps for relevant pool in last 7 days
swaps AS (
  SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  contract_address,
  event_name,
  event_inputs,
  event_inputs :amount0In :: INTEGER AS amount0In,
  event_inputs :amount0Out :: INTEGER AS amount0Out,
  event_inputs: amount1In :: INTEGER AS amount1In,
  event_inputs: amount1Out :: INTEGER AS amount1Out,
  event_inputs: sender :: STRING AS sender,
  event_inputs: to :: STRING AS to_address
FROM ethereum.core.fact_event_logs
WHERE contract_address = lower('0x397FF1542f962076d0BFE58eA045FfA2d347ACa0')
  AND event_name IN ('Swap')
  AND block_timestamp >= CURRENT_DATE -6 
),
-- aggregate pool, token and swap details
swaps_contract_details AS (
  SELECT
       block_number,
       block_timestamp,
       tx_hash,
       event_index,
       contract_address,
       amount0In,
       amount0Out,
       amount1In,
       amount1Out,
       sender,
       to_address,
       pool_name,
       pool_address,
       token0,
       token1,
       token0symbol,
       token1symbol,
       token0decimals,
       token1decimals
FROM swaps
  LEFT JOIN pool_token_details
  ON contract_address = pool_address
),
-- transform amounts by respective token decimals
final_details AS (
  SELECT
  	   pool_name,
       pool_address,
       block_number,
       block_timestamp,
       tx_hash,
  	   amount0In / pow(10, token0decimals) AS amount0In_ADJ,
  	   amount0Out / pow(10, token0decimals) AS amount0Out_ADJ,
  	   amount1In / pow(10, token1decimals) AS amount1In_ADJ,
  	   amount1Out / pow(10, token1decimals) AS amount1Out_ADJ,
  	   token0symbol,
  	   token1symbol
  FROM swaps_contract_details
)
SELECT
  date_trunc('day', block_timestamp) AS DATE,
  sum(amount0In_ADJ) + sum(amount0Out_ADJ) AS TOTAL_USDC_VOL,
  count(tx_hash) AS USDC_SWAP_COUNT
FROM final_details
GROUP BY DATE
ORDER BY DATE
