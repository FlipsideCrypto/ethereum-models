{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH pool_meta AS (

    SELECT
        DISTINCT pool_address,
        pool_name,
        token_address,
        pool_symbol AS symbol,
        token_id :: INTEGER AS token_id,
        token_type :: STRING AS token_type
    FROM
        {{ ref('silver_dex__curve_pools') }}
),
pools AS (
    SELECT
        DISTINCT pool_address,
        pool_name
    FROM
        pool_meta qualify (ROW_NUMBER() over (PARTITION BY pool_address
    ORDER BY
        pool_name ASC nulls last)) = 1
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
        CASE
            WHEN topics [0] :: STRING = '0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b' THEN 'TokenExchangeUnderlying'
            ELSE 'TokenExchange'
        END AS event_name,
        contract_address AS pool_address,
        pool_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS sold_id,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS tokens_sold,
        TRY_CAST(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) AS INTEGER
        ) AS bought_id,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS tokens_bought,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING IN (
            '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140',
            '0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98',
            '0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b',
            '0x143f1f8e861fbdeddd5b46e844b7d3ac7b86a122f36e8c463859ee6811b1f29c'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
token_exchange AS (
    SELECT
        _log_id,
        MAX(
            CASE
                WHEN sold_id = token_id THEN token_address
            END
        ) AS token_in,
        MAX(
            CASE
                WHEN bought_id = token_id THEN token_address
            END
        ) AS token_out,
        MAX(
            CASE
                WHEN sold_id = token_id THEN symbol
            END
        ) AS symbol_in,
        MAX(
            CASE
                WHEN bought_id = token_id THEN symbol
            END
        ) AS symbol_out
    FROM
        curve_base t
        LEFT JOIN pool_meta p
        ON p.pool_address = t.pool_address
        AND (
            p.token_id = t.sold_id
            OR p.token_id = t.bought_id
        )
    WHERE
        token_type = 'coins'
    GROUP BY
        1
),
token_transfers AS (
    SELECT
        tx_hash,
        contract_address AS token_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
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
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
        COALESCE(
            sold.token_address,
            e.token_in
        ) AS token_in,
        e.symbol_in AS symbol_in,
        bought_id,
        tokens_bought,
        COALESCE(
            bought.token_address,
            e.token_out
        ) AS token_out,
        e.symbol_out AS symbol_out,
        s._log_id,
        _inserted_timestamp
    FROM
        curve_base s
        LEFT JOIN token_exchange e
        ON s._log_id = e._log_id
        LEFT JOIN from_transfers sold
        ON tokens_sold = sold.amount
        AND s.tx_hash = sold.tx_hash
        LEFT JOIN to_transfers bought
        ON tokens_bought = bought.amount
        AND s.tx_hash = bought.tx_hash
    WHERE
        tokens_sold <> 0 qualify(ROW_NUMBER() over(PARTITION BY s._log_id
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
