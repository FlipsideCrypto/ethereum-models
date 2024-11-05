{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

with flashloan AS(

    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS caller,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token,
        try_to_number(
        utils.udf_hex_to_int(
                segmented_data [0] :: STRING
         ))
        AS flashloan_quantity,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }} 
        l 
    WHERE
        topics[0]::STRING = '0xc76f1b4fe4396ac07a9fa55a415d4ca430e72651d37d3401f3bed7cb13fc4f12'
        AND contract_address = '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb'
{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    caller as initiator_address,
    token as market,
    c.symbol,
    flashloan_quantity AS flashloan_amount_unadj,
    flashloan_quantity / pow(
        10,
        c.decimals
    ) AS flashloan_amount,
    'Morpho Blue' AS platform,
    'ethereum' AS blockchain,
    f._log_id,
    f._inserted_timestamp
FROM
    flashloan f
    LEFT JOIN {{ ref('silver__contracts') }} c
    ON  f.token = c.address