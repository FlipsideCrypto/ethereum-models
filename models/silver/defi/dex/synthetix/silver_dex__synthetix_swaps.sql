{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH swaps_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        NULL AS pool_name,
        'SynthExchange' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS amount_in_unadj,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS amount_out_unadj,
        REGEXP_REPLACE(
            utils.udf_hex_to_string(
                segmented_data [0] :: STRING
            ),
            '[^a-zA-Z0-9]+'
        ) AS symbol_in,
        REGEXP_REPLACE(
            utils.udf_hex_to_string(
                segmented_data [2] :: STRING
            ),
            '[^a-zA-Z0-9]+'
        ) AS symbol_out,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS tx_to,
        event_index,
        'synthetix' AS platform,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f',
            '0xc011a72400e58ecd99ee497cf89e3775d4bd732f'
        ) -- synthetix proxy contracts (new / old)
        AND topics [0] = '0x65b6972c94204d84cffd3a95615743e31270f04fdf251f3dccc705cfbad44776'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
    contract_address,
    pool_name,
    event_name,
    sender,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    tx_to,
    event_index,
    platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base s
    LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_in,
            synth_proxy_address AS token_in,
            decimals AS decimals_in
        FROM
            {{ ref('silver__synthetix_synths_20230313') }}
    ) sc1
    ON sc1.synth_symbol_in = s.symbol_in
    LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_out,
            synth_proxy_address AS token_out,
            decimals AS decimals_out
        FROM
            {{ ref('silver__synthetix_synths_20230313') }}
    ) sc2
    ON sc2.synth_symbol_out = s.symbol_out
