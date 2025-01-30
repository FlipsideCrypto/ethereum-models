{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH pools AS (

    SELECT
        DISTINCT pool_address
    FROM
        {{ ref('silver_dex__dodo_v2_pools') }}
),
proxies AS (
    SELECT
        '0x91e1c84ba8786b1fae2570202f0126c0b88f6ec7' AS proxy_address
    UNION
    SELECT
        '0x9b64c81ba54ea51e1f6b7fefb3cff8aa6f1e2a09'
    UNION
    SELECT
        '0xe6aafa1c45d9d0c64686c1f1d17b9fe9c7dab05b'
    UNION
    SELECT
        '0xe55154d09265b18ac7cdac6e646672a5460389a1'
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
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
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
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
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
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
