{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pool_name AS (

    SELECT
        CASE
            WHEN pool_name IS NULL THEN pool_symbol
            ELSE pool_name 
        END AS pool_name,
        pool_address
    FROM
        {{ ref('silver_dex__balancer_pools') }}
),
swaps_base AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        _inserted_timestamp,
        'Swap' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        (CASE 
            WHEN segmented_data [0] = '0x' THEN NULL 
            ELSE ethereum.public.udf_hex_to_int(
            segmented_data [0] :: STRING
                )
            END) :: INTEGER AS amount_in_unadj,
        (CASE 
            WHEN segmented_data [1] = '0x' THEN NULL 
            ELSE ethereum.public.udf_hex_to_int(
            segmented_data [1] :: STRING
                ) 
            END) :: INTEGER AS amount_out_unadj,
        topics [1] :: STRING AS pool_id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_in,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_out,
        SUBSTR(topics [1] :: STRING,1,42) AS pool_address,
        _log_id,
        ingested_at,
        'balancer' AS platform,
        origin_from_address AS sender,
        origin_from_address AS tx_to
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics[0]::STRING = '0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b'
        AND contract_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        decimals IS NOT NULL
        AND 
            address IN (
                SELECT
                    DISTINCT token_in AS address
                FROM
                    swaps_base
                UNION
                SELECT
                    DISTINCT token_out AS address
                FROM
                    swaps_base
        )
),
hourly_token_price AS (
    SELECT
        hour,
        token_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT address
            FROM
                contracts
        )
        AND hour :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                swaps_base
        )
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    _inserted_timestamp,
    s.event_name,
    event_index,
    amount_in_unadj,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            amount_in * p1.price,
            2
        )
    END AS amount_in_usd,
    amount_out_unadj,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(
            amount_out * p2.price,
            2
        )
    END AS amount_out_usd,
    s.pool_id,
    token_in,
    token_out,
    s.pool_address,
    s._log_id,
    s.ingested_at,
    s.platform,
    sender,
    tx_to,
    pool_name
FROM
    swaps_base s
    LEFT JOIN contracts c1
    ON token_in = c1.address
    LEFT JOIN contracts c2
    ON token_out = c2.address
    LEFT JOIN hourly_token_price p1
    ON token_in = p1.token_address
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p1.hour
    LEFT JOIN hourly_token_price p2
    ON token_out = p2.token_address
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p2.hour
    LEFT JOIN pool_name pn
    ON pn.pool_address = s.pool_address
WHERE
    pool_name IS NOT NULL
