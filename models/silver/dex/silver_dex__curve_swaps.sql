{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pool_meta AS (

    SELECT
        DISTINCT pool_address,
        CASE 
            WHEN pool_name IS NULL AND pool_symbol IS NULL THEN CONCAT('Curve.fi Pool: ',SUBSTRING(pool_address, 1, 5),'...',SUBSTRING(pool_address, 39, 42))
            WHEN pool_name IS NULL THEN CONCAT('Curve.fi Pool: ',replace(regexp_replace(agg_symbol, '[^[:alnum:],]', '', 1, 0), ',', '-'))
        ELSE pool_name
    END AS pool_name,
    token_address,
    pool_symbol AS symbol,
    token_id::INTEGER AS token_id,
    token_type::STRING AS token_type
    FROM
        {{ ref('silver_dex__curve_pools') }}
    LEFT JOIN (
        SELECT
            pool_address,
            array_agg(pool_symbol)::STRING AS agg_symbol
        FROM {{ ref('silver_dex__curve_pools') }}
        GROUP BY 1
        ) USING(pool_address)
),

pools AS (

SELECT 
	DISTINCT pool_address,
	pool_name
FROM pool_meta
),

curve_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        event_name,
        contract_address AS pool_address,
        pool_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS sold_id,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS tokens_sold,
        TRY_CAST(PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS INTEGER) AS bought_id,
        PUBLIC.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS tokens_bought,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        INNER JOIN pools
        ON pools.pool_address = contract_address
    WHERE
        topics [0] :: STRING IN (
            '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140',
            '0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98',
            '0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),

token_exchange AS (

SELECT
	_log_id,
    MAX(CASE WHEN sold_id = token_id THEN token_address END) AS token_in,
    MAX(CASE WHEN bought_id = token_id THEN token_address END) AS token_out,
    MAX(CASE WHEN sold_id = token_id THEN symbol END) AS symbol_in,
    MAX(CASE WHEN bought_id = token_id THEN symbol END) AS symbol_out
FROM curve_base t
LEFT JOIN pool_meta p ON p.pool_address = t.pool_address AND (p.token_id = t.sold_id OR p.token_id = t.bought_id)
WHERE token_type = 'coins'
GROUP BY 1
),

token_transfers AS (
    SELECT
        tx_hash,
        contract_address AS token_address,
        TRY_TO_NUMBER(
            PUBLIC.udf_hex_to_int(
                DATA :: STRING
            )
        ) AS amount,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                curve_base
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) <> '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
from_transfers AS (
    SELECT
        DISTINCT tx_hash,
        token_address,
        from_address,
        amount
    FROM
        token_transfers
),
to_transfers AS (
    SELECT
        DISTINCT tx_hash,
        token_address,
        to_address,
        amount
    FROM
        token_transfers
),

ready_pool_info AS (

SELECT
	s.block_number,
    s.block_timestamp,
    s.tx_hash,
    s.origin_function_signature,
    s.origin_from_address,
    s.origin_from_address AS tx_to,
    s.origin_to_address,
    event_index,
    event_name,
    pool_address,
    pool_address AS contract_address,
    pool_name,
    sender,
    sold_id,
    tokens_sold,
    COALESCE(sold.token_address,e.token_in,'0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token_in,
    e.symbol_in AS symbol_in,
    bought_id,
    tokens_bought,
    COALESCE(bought.token_address,e.token_out,'0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS token_out,
    e.symbol_out AS symbol_out,
    s._log_id,
    _inserted_timestamp
FROM
    curve_base s
    LEFT JOIN token_exchange e ON s._log_id = e._log_id
    LEFT JOIN from_transfers sold
    ON tokens_sold = sold.amount
    AND s.tx_hash = sold.tx_hash
    LEFT JOIN to_transfers bought
    ON tokens_bought = bought.amount
    AND s.tx_hash = bought.tx_hash
WHERE
	tokens_sold <> 0
qualify(ROW_NUMBER() over(PARTITION BY s._log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1  
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    tx_to,
    origin_to_address,
    event_index,
    event_name,
    pool_address,
    contract_address,
    pool_name,
    sender,
    sold_id,
    tokens_sold,
    token_in,
    symbol_in,
    bought_id,
    tokens_bought,
    token_out,
    symbol_out,
    _log_id,
    _inserted_timestamp,
    'curve' AS platform
FROM
    ready_pool_info

----- draft

-- pool_info AS (
--     SELECT
--         block_number,
--         block_timestamp,
--         tx_hash,
--         origin_function_signature,
--         origin_from_address,
--         tx_to,
--         origin_to_address,
--         event_index,
--         event_name,
--         pool_address,
--         contract_address,
--         pool_name,
--         sender,
--         sold_id,
--         tokens_sold,
--         token_in,
--         COALESCE(c0.symbol,r.symbol_in) AS symbol_in,
--         c0.decimals AS decimals_in,
--         CASE
--             WHEN decimals_in IS NOT NULL THEN tokens_sold / pow(
--                 10,
--                 decimals_in
--             )
--             ELSE tokens_sold
--         END AS amount_in,
--         bought_id,
--         tokens_bought,
--         token_out,
--         COALESCE(c1.symbol,r.symbol_out) AS symbol_out,
--         c1.decimals AS decimals_out,
--         CASE
--             WHEN decimals_out IS NOT NULL THEN tokens_bought / pow(
--                 10,
--                 decimals_out
--             )
--             ELSE tokens_bought
--         END AS amount_out,
--         _log_id,
--         _inserted_timestamp
--     FROM
--         ready_pool_info r
--         LEFT JOIN ETHEREUM.core.dim_contracts
--         c0
--         ON c0.address = r.token_in
--         LEFT JOIN ETHEREUM.core.dim_contracts
--         c1
--         ON c1.address = r.token_out
--     WHERE amount_out <> 0
-- ),

-- prices AS (
--     SELECT
--         HOUR,
--         token_address,
--         price
--     FROM
--         {{ ref('core__fact_hourly_token_prices') }}
--     WHERE
--         HOUR :: DATE IN (
--             SELECT
--                 DISTINCT block_timestamp :: DATE
--             FROM
--                 curve_base
--         )
--         AND (
--             token_address IN (
--                 SELECT
--                     DISTINCT token_in
--                 FROM
--                     pool_info
--             )
--             OR token_address IN (
--                 SELECT
--                     DISTINCT token_out
--                 FROM
--                     pool_info
--             )
--         )
-- )

-- SELECT
--     block_number,
--     block_timestamp,
--     origin_function_signature,
--     origin_from_address,
--     origin_to_address,
--     contract_address,
--     tx_hash,
--     event_index,
--     event_name,
--     tx_to,
--     pool_address,
--     pool_name,
--     sender,
--     decimals_in,
--     symbol_in,
--     token_in,
--     amount_in,
--     decimals_out,
--     symbol_out,
--     token_out,
--     amount_out,
--     CASE
--         WHEN decimals_in IS NOT NULL THEN ROUND(
--             amount_in * p0.price,
--             2
--         )
--     END AS amount_in_usd,
--     CASE
--         WHEN decimals_out IS NOT NULL THEN ROUND(
--             amount_out * p1.price,
--             2
--         )
--     END AS amount_out_usd,
--     _log_id,
--     _inserted_timestamp,
--     'curve' AS platform
-- FROM
--     pool_info
--     LEFT JOIN prices p0
--     ON p0.hour = DATE_TRUNC(
--         'hour',
--         block_timestamp
--     )
--     AND p0.token_address = token_in
--     LEFT JOIN prices p1
--     ON p1.hour = DATE_TRUNC(
--         'hour',
--         block_timestamp
--     )
--     AND p1.token_address = token_out
-- WHERE
--     COALESCE(symbol_in,'null') <> COALESCE(symbol_out,'null')