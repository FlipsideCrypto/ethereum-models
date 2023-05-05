{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pools AS (

    SELECT
        '0x813fddeccd0401c4fa73b092b074802440544e52' AS pool_address,
        'USDC' AS base_token_symbol,
        'USDT' AS quote_token_symbol,
        '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' AS base_token,
        '0xc2132d05d31c914a87c6611c10748aeb04b58e8f' AS quote_token
),
proxies AS (
    SELECT
        '0xdbfaf391c37339c903503495395ad7d6b096e192' AS proxy_address
    UNION
    SELECT
        '0x6c30be15d88462b788dea7c6a860a2ccaf7b2670' AS proxy_address
),
sell_base_token AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS seller_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS payBase,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS receiveQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        receiveQuote AS amountIn,
        payBase AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xd8648b6ac54162763c86fd54bf2005af8ecd2f9cb273a5775921fd7f91e17b2d' --sellBaseToken
        AND seller_address NOT IN (
            SELECT
                proxy_address
            FROM
                proxies
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
buy_base_token AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS buyer_address,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS receiveBase,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS payQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        payQuote AS amountIn,
        receiveBase AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3' --buyBaseToken
        AND buyer_address NOT IN (
            SELECT
                proxy_address
            FROM
                proxies
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    seller_address AS sender,
    origin_from_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'SellBaseToken' AS event_name,
    'dodo-v1' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    sell_base_token
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    buyer_address AS sender,
    origin_from_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'BuyBaseToken' AS event_name,
    'dodo-v1' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    buy_base_token
