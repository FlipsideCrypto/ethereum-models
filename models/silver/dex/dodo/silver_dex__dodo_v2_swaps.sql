{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pools AS (

    SELECT
        DISTINCT pool_address
    FROM
        {{ ref('silver_dex__dodo_v2_pools') }}
),
proxies AS (
    SELECT
        '0xdbfaf391c37339c903503495395ad7d6b096e192' AS proxy_address
    UNION
    SELECT
        '0x6c30be15d88462b788dea7c6a860a2ccaf7b2670' AS proxy_address
),
swaps_base AS (
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
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS fromToken,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [1] :: STRING,
                25,
                40
            )
        ) AS toToken,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [4] :: STRING,
                25,
                40
            )
        ) AS trader_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [5] :: STRING,
                25,
                40
            )
        ) AS receiver_address,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    INNER JOIN pools p
    ON
        l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f' --dodoswap
        AND trader_address NOT IN (
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
    fromToken AS token_in,
    toToken AS token_out,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    trader_address AS sender,
    receiver_address AS tx_to,
    'DodoSwap' AS event_name,
    'dodo-v2' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
