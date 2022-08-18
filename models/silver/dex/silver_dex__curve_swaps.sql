{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pool_meta AS (

    SELECT
        pool_address,
        token_address,
        token_index,
        token_decimals,
        token_symbol,
        pool_name
    FROM
        {{ ref('silver_dex__curve_pools') }}
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
    WHERE
        topics [0] :: STRING IN (
            '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140',
            '0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98'
        )
        AND contract_address IN (
            SELECT
                DISTINCT pool_address
            FROM
                pool_meta
        ) -- we will only include pools in the underlying pools table
        -- if one of relevance is missing it can be added to the seed file until the reads lambda is ready

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
pool_info AS (
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
        origin_from_address AS tx_to,
        curve_base.pool_address AS pool_address,
        pool0.pool_name AS pool_name,
        sender,
        sold_id,
        tokens_sold AS amount_in_unadj,
        pool0.token_decimals AS decimals_in,
        pool0.token_symbol AS symbol_in,
        pool0.token_address AS token_in,
        CASE
            WHEN decimals_in IS NOT NULL THEN amount_in_unadj / pow(
                10,
                decimals_in
            )
            ELSE amount_in_unadj
        END AS amount_in,
        bought_id,
        tokens_bought AS amount_out_unadj,
        pool1.token_decimals AS decimals_out,
        pool1.token_symbol AS symbol_out,
        pool1.token_address AS token_out,
        CASE
            WHEN decimals_out IS NOT NULL THEN amount_out_unadj / pow(
                10,
                decimals_out
            )
            ELSE amount_out_unadj
        END AS amount_out,
        _log_id,
        _inserted_timestamp
    FROM
        curve_base
        LEFT JOIN pool_meta pool0
        ON curve_base.pool_address = pool0.pool_address
        AND sold_id = pool0.token_index
        LEFT JOIN pool_meta pool1
        ON curve_base.pool_address = pool1.pool_address
        AND bought_id = pool1.token_index
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
