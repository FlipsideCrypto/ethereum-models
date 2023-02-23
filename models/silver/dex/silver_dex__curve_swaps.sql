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
    END AS pool_name
    FROM
        {{ ref('silver_dex__curve_pools_temp') }}
    LEFT JOIN (
        SELECT
            pool_address,
            array_agg(pool_symbol)::STRING AS agg_symbol
        FROM {{ ref('silver_dex__curve_pools_temp') }}
        GROUP BY 1
        ) USING(pool_address)
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
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS bought_id,
        PUBLIC.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS tokens_bought,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        INNER JOIN pool_meta
        ON pool_meta.pool_address = contract_address
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
        AND CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) <> '0x0000000000000000000000000000000000000000'
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
pool_info AS (
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
        COALESCE(
            sold.token_address,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) AS token_in,
        c0.symbol symbol_in,
        c0.decimals AS decimals_in,
        CASE
            WHEN decimals_in IS NOT NULL THEN tokens_sold / pow(
                10,
                decimals_in
            )
            ELSE tokens_sold
        END AS amount_in,
        bought_id,
        tokens_bought,
        COALESCE(
            bought.token_address,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ) AS token_out,
        c1.symbol AS symbol_out,
        c1.decimals AS decimals_out,
        CASE
            WHEN decimals_out IS NOT NULL THEN tokens_bought / pow(
                10,
                decimals_out
            )
            ELSE tokens_bought
        END AS amount_out,
        _log_id,
        _inserted_timestamp
    FROM
        curve_base s
        LEFT JOIN from_transfers sold
        ON tokens_sold = sold.amount
        AND s.tx_hash = sold.tx_hash --  AND s.pool_address = sold.from_address
        LEFT JOIN to_transfers bought
        ON tokens_bought = bought.amount
        AND s.tx_hash = bought.tx_hash -- AND s.pool_address = bought.to_address
        LEFT JOIN {{ ref('core__dim_contracts') }}
        c0
        ON c0.address = COALESCE(
            sold.token_address,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        )
        LEFT JOIN {{ ref('core__dim_contracts') }}
        c1
        ON c1.address = COALESCE(
            bought.token_address,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        )
    WHERE
        tokens_sold <> 0
        AND symbol_out <> symbol_in qualify(ROW_NUMBER() over(PARTITION BY _log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
prices AS (
    SELECT
        HOUR,
        LOWER(token_address) AS token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                curve_base
        )
        AND (
            token_address IN (
                SELECT
                    DISTINCT token_in
                FROM
                    pool_info
            )
            OR token_address IN (
                SELECT
                    DISTINCT token_out
                FROM
                    pool_info
            )
        )
    GROUP BY
        1,
        2
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    tx_hash,
    event_index,
    event_name,
    tx_to,
    pool_address,
    pool_name,
    sender,
    decimals_in,
    symbol_in,
    token_in,
    amount_in,
    decimals_out,
    symbol_out,
    token_out,
    amount_out,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            amount_in * p0.price,
            2
        )
    END AS amount_in_usd,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(
            amount_out * p1.price,
            2
        )
    END AS amount_out_usd,
    _log_id,
    _inserted_timestamp,
    'curve' AS platform
FROM
    pool_info
    LEFT JOIN prices p0
    ON p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND p0.token_address = token_in
    LEFT JOIN prices p1
    ON p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND p1.token_address = token_out
