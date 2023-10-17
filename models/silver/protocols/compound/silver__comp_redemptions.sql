{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}
-- pull all ctoken addresses and corresponding name
WITH asset_details AS (

    SELECT
        ctoken_address,
        ctoken_symbol,
        ctoken_name,
        ctoken_decimals,
        underlying_asset_address,
        ctoken_metadata,
        underlying_name,
        underlying_symbol,
        underlying_decimals,
        underlying_contract_metadata,
        compound_version
    FROM
        {{ ref('silver__comp_asset_details') }}
),
compv2_redemptions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address AS ctoken,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS received_amount_raw,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS redeemed_ctoken_raw,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS redeemer,
        'Compound V2' AS compound_version,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            SELECT
                ctoken_address
            FROM
                asset_details
            WHERE compound_version = 'Compound V2'
        )
        AND topics [0] :: STRING = '0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
compv3_redemptions AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS ctoken,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS received_amount_raw,
        NULL AS redeemed_ctoken_raw,
        origin_from_address AS redeemer,
        'Compound V3' AS compound_version,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] = '0xd6d480d5b3068db003533b170d67561494d72e3bf9fa40a266471351ebba9e16' --WithdrawCollateral
        AND contract_address IN (
            '0xa17581a9e3356d9a858b789d68b4d866e593ae94',
            '0xc3d688b66703497daa19211eedff47f25384cdc3'
        )

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
comp_combine AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        ctoken,
        redeemer,
        received_amount_raw,
        redeemed_ctoken_raw,
        C.underlying_asset_address AS received_contract_address,
        C.underlying_symbol AS received_contract_symbol,
        C.ctoken_symbol,
        C.ctoken_decimals,
        C.underlying_decimals,
        b.compound_version,
        b._log_id,
        b._inserted_timestamp
    FROM
        compv2_redemptions b
        LEFT JOIN {{ ref('silver__comp_asset_details') }} C
        ON b.ctoken = C.ctoken_address
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        ctoken,
        redeemer,
        received_amount_raw,
        redeemed_ctoken_raw,
        C.underlying_asset_address AS received_contract_address,
        C.underlying_symbol AS recieved_contract_symbol,
        C.ctoken_symbol,
        C.ctoken_decimals,
        C.underlying_decimals,
        b.compound_version,
        b._log_id,
        b._inserted_timestamp
    FROM
        compv3_redemptions b
        LEFT JOIN {{ ref('silver__comp_asset_details') }} C
        ON b.ctoken = C.ctoken_address
),
--pull hourly prices for each underlying
prices AS (
    SELECT
        HOUR AS block_hour,
        token_address AS token_contract,
        ctoken_address,
        AVG(price) AS token_price
    FROM
        {{ ref('price__ez_hourly_token_prices') }}
        INNER JOIN asset_details
        ON token_address = underlying_asset_address
    WHERE
        HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                comp_combine
        )
    GROUP BY
        1,
        2,
        3
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    ctoken,
    ctoken_symbol,
    received_amount_raw / pow(
        10,
        underlying_decimals
    ) AS received_amount,
    ROUND(
        received_amount * p.token_price,
        2
    ) AS received_amount_usd,
    received_contract_address,
    received_contract_symbol,
    redeemed_ctoken_raw / pow(
        10,
        ctoken_decimals
    ) AS redeemed_ctoken,
    redeemer,
    compound_version,
    _inserted_timestamp,
    _log_id
FROM
    comp_combine ee
    LEFT JOIN prices p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p.block_hour
    AND ee.ctoken = p.ctoken_address
