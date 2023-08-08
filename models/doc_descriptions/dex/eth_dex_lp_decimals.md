{% docs eth_dex_lp_decimals %}

The # of decimals for the token included in the liquidity pool, as a JSON object, where available. 

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM ethereum.defi.dim_dex_liquidity_pools
WHERE token0_decimal = 6
;

{% enddocs %}