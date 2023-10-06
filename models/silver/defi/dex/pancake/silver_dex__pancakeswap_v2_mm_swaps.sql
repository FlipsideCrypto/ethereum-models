{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH swaps_base AS (

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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS user_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS mm_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS nonce,
        CONCAT('0x', SUBSTR(l_segmented_data [1] :: STRING, 25, 40)) AS mmTreasury,
        CONCAT('0x', SUBSTR(l_segmented_data [2] :: STRING, 25, 40)) AS baseToken1,
        CONCAT('0x', SUBSTR(l_segmented_data [3] :: STRING, 25, 40)) AS quoteToken1,
        CASE
            WHEN baseToken1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE baseToken1
        END AS baseToken,
        CASE
            WHEN quoteToken1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE quoteToken1
        END AS quoteToken,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS baseTokenAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [5] :: STRING
            )
        ) AS quoteTokenAmount,
        baseToken AS token_in,
        quoteToken AS token_out,
        baseTokenAmount AS token_in_amount,
        quoteTokenAmount AS token_out_amount,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address = '0x9ca2a439810524250e543ba8fb6e88578af242bc' --router
        AND topics [0] :: STRING = '0xe7d6f812e1a54298ddef0b881cd08a4d452d9de35eb18b5145aa580fdda18b26' --swap

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
    user_address AS sender,
    user_address AS tx_to,
    mm_address,
    nonce,
    mmTreasury,
    baseToken,
    quoteToken,
    baseTokenAmount,
    quoteTokenAmount,
    token_in,
    token_out,
    token_in_amount AS amount_in_unadj,
    token_out_amount AS amount_out_unadj,
    'Swap' AS event_name,
    'pancakeswap-v2' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
