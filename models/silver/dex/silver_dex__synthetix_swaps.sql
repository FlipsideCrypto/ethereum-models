{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
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
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS amount_in_unadj,
        PUBLIC.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS amount_out_unadj,
        REGEXP_REPLACE(HEX_DECODE_STRING(
            segmented_data [0] :: STRING
        ),'[^a-zA-Z0-9]+') AS symbol_in,
        REGEXP_REPLACE(HEX_DECODE_STRING(
            segmented_data [2] :: STRING
        ),'[^a-zA-Z0-9]+') AS symbol_out,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40))AS tx_to,
        event_index,
        'synthetix' AS platform,
        CONCAT(tx_hash,'-',event_index) AS _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f',
            '0xc011a72400e58ecd99ee497cf89e3775d4bd732f'
        ) -- synthetix proxy contracts (new / old)
        AND topics[0] = '0x65b6972c94204d84cffd3a95615743e31270f04fdf251f3dccc705cfbad44776'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
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
    symbol_in,
    symbol_out,
    tx_to,
    event_index,
    platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base

--draft    
--     LEFT JOIN (
--         SELECT
--             synth_symbol AS synth_symbol_in,
--             synth_proxy_address AS token_in,
--             decimals AS decimals_in
--         FROM
--             {{ ref('silver__synthetix_synths_20230313') }}
--     ) synths_in
--     ON synths_in.synth_symbol_in = b.symbol_in
--     LEFT JOIN (
--         SELECT
--             synth_symbol AS synth_symbol_out,
--             synth_proxy_address AS token_out,
--             decimals AS decimals_out
--         FROM
--             {{ ref('silver__synthetix_synths_20230313') }}
--     ) synths_out
--     ON synths_out.synth_symbol_out = b.symbol_out


-- hourly_prices AS (
--     SELECT
--         hour,
--         token_address,
--         price
--     FROM
--         {{ ref('core__fact_hourly_token_prices') }}
--     WHERE hour :: DATE IN (
--         SELECT
--             DISTINCT block_timestamp :: DATE
--         FROM
--             swaps_base
--     )
--     AND token_address IN (
--         SELECT
--             DISTINCT token_in AS token_address
--         FROM swaps_with_token_addresses
--         UNION
--         SELECT
--             DISTINCT token_out AS token_address
--         FROM swaps_with_token_addresses
--     )
-- )

--     SELECT
--         block_number,
--         block_timestamp,
--         tx_hash,
--         origin_function_signature,
--         origin_from_address,
--         origin_to_address,
--         contract_address,
--         pool_name,
--         event_name,
--         CASE
--             WHEN decimals_in IS NOT NULL THEN amount_in_unadj / pow(10,decimals_in)
--             ELSE amount_in_unadj
--         END AS amount_in ,
--         amount_in * prices_in.price AS amount_in_usd,
--         CASE
--             WHEN decimals_out IS NOT NULL THEN amount_out_unadj / pow(10,decimals_out)
--             ELSE amount_out_unadj
--         END AS amount_out ,
--         amount_out * prices_out.price AS amount_out_usd,
--         sender,
--         tx_to,
--         event_index,
--         platform,
--         token_in,
--         token_out,
--         symbol_in,
--         symbol_out,
--         _log_id,
--         _inserted_timestamp
--     FROM
--         swaps_with_token_addresses s
--         LEFT JOIN hourly_prices AS prices_in
--             ON prices_in.token_address = s.token_in
--                     AND prices_in.hour = date_trunc('hour',s.block_timestamp)
--         LEFT JOIN hourly_prices AS prices_out
--             ON prices_out.token_address = s.token_out
--                     AND prices_out.hour = date_trunc('hour',s.block_timestamp)
