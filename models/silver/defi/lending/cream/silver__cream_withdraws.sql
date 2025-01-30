{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}
-- pull all token addresses and corresponding name
WITH asset_details AS (

    SELECT
        token_address,
        token_symbol,
        token_name,
        token_decimals,
        underlying_asset_address,
        underlying_name,
        underlying_symbol,
        underlying_decimals
    FROM
        {{ ref('silver__cream_asset_details') }}
),
cream_redemptions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        contract_address AS token,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS received_amount_raw,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS redeemed_token_raw,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS redeemer,
        'Cream' AS platform,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            SELECT
                token_address
            FROM
                asset_details
        )
        AND topics [0] :: STRING = '0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929'
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
),
cream_combine AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        token,
        redeemer,
        received_amount_raw,
        redeemed_token_raw,
        C.underlying_asset_address AS received_contract_address,
        C.underlying_symbol AS received_contract_symbol,
        C.token_symbol,
        C.token_decimals,
        C.underlying_decimals,
        b.platform,
        b._log_id,
        b._inserted_timestamp
    FROM
        cream_redemptions b
        LEFT JOIN asset_details C
        ON b.token = C.token_address
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
    token as token_address,
    token_symbol,
    received_amount_raw AS amount_unadj,
    received_amount_raw / pow(
        10,
        underlying_decimals
    ) AS amount,
    received_contract_address,
    received_contract_symbol,
    redeemed_token_raw / pow(
        10,
        token_decimals
    ) AS redeemed_token,
    redeemer,
    platform,
    _inserted_timestamp,
    _log_id
FROM
    cream_combine ee qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
