{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND',
                'PURPOSE': 'DEFI'
            }
        }
    },
    tags = ['non_realtime'],
    persist_docs ={ "relation": true,
    "columns": true }
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
        underlying_contract_metadata
    FROM
        {{ ref('compound__ez_asset_details') }}
),
comp_redemptions AS (
    SELECT
        block_number,
        block_timestamp,
        contract_address AS ctoken,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS received_amount_raw,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS redeemed_ctoken_raw,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS redeemer,
        tx_hash,
        event_index,
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
        )
        AND topics [0] :: STRING = '0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
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
                comp_redemptions
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
    asset_details.ctoken_symbol AS ctoken_symbol,
    received_amount_raw / pow(
        10,
        underlying_decimals
    ) AS received_amount,
    ROUND(
        received_amount * p.token_price,
        2
    ) AS received_amount_usd,
    CASE
        WHEN asset_details.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN NULL
        ELSE asset_details.underlying_asset_address
    END AS received_contract_address,
    CASE
        WHEN asset_details.underlying_asset_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'ETH'
        ELSE asset_details.underlying_symbol
    END AS received_contract_symbol,
    redeemed_ctoken_raw / pow(
        10,
        ctoken_decimals
    ) AS redeemed_ctoken,
    redeemer,
    _inserted_timestamp,
    _log_id
FROM
    comp_redemptions ee
    LEFT JOIN prices p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p.block_hour
    AND ee.ctoken = p.ctoken_address
    LEFT JOIN asset_details
    ON ee.ctoken = asset_details.ctoken_address
