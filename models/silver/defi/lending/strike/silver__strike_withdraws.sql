{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}
-- pull all itoken addresses and corresponding name
WITH asset_details AS (

    SELECT
        itoken_address,
        itoken_symbol,
        itoken_name,
        itoken_decimals,
        underlying_asset_address,
        underlying_name,
        underlying_symbol,
        underlying_decimals
    FROM
        {{ ref('silver__strike_asset_details') }}
),
strike_redemptions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        contract_address AS itoken,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS received_amount_raw,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS redeemed_itoken_raw,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS redeemer,
        'Strike' AS platform,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            SELECT
                itoken_address
            FROM
                asset_details
        )
        AND topics [0] :: STRING = '0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
strike_combine AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        itoken,
        redeemer,
        received_amount_raw,
        redeemed_itoken_raw,
        C.underlying_asset_address AS received_contract_address,
        C.underlying_symbol AS received_contract_symbol,
        C.itoken_symbol,
        C.itoken_decimals,
        C.underlying_decimals,
        b.platform,
        b._log_id,
        b._inserted_timestamp
    FROM
        strike_redemptions b
        LEFT JOIN asset_details c
        ON b.itoken = C.itoken_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    itoken,
    itoken_symbol,
    received_amount_raw AS amount_unadj,
    received_amount_raw / pow(
        10,
        underlying_decimals
    ) AS received_amount,
    received_contract_address,
    received_contract_symbol,
    redeemed_itoken_raw / pow(
        10,
        itoken_decimals
    ) AS redeemed_itoken,
    redeemer,
    platform,
    _inserted_timestamp,
    _log_id
FROM
    strike_combine ee qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
