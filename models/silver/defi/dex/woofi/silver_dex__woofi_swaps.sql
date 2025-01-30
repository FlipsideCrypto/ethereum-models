{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH router_swaps_base AS (

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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS from_token,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_token,
        CONCAT('0x', SUBSTR(l.topics [3] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS swapType,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS from_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [4] :: STRING,
                25,
                40
            )
        ) AS rebateTo,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        contract_address IN (
            '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7',
            '0x044c08639bd59beb4f6ec52c0da6cd47283534e8' --v2
        )
        AND topics [0] :: STRING = '0x27c98e911efdd224f4002f6cd831c3ad0d2759ee176f9ee8466d95826af22a1c' --WooRouterSwap
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
    CASE
        WHEN from_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE from_token
    END AS token_in,
    CASE
        WHEN to_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE to_token
    END AS token_out,
    to_address AS tx_to,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'WooRouterSwap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    router_swaps_base