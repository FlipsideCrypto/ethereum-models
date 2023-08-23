{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

WITH asset_details AS (
    SELECT
        address :: STRING AS ctoken_address,
        symbol :: STRING AS ctoken_symbol,
        NAME :: STRING AS ctoken_name,
        decimals :: INTEGER AS ctoken_decimals,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS underlying_asset_address,
        'WETH' AS underlying_symbol,
        18 AS underlying_decimals,
        contract_metadata
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        address = '0xa17581a9e3356d9a858b789d68b4d866e593ae94'
),
comp_withdrawls AS (
    SELECT
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS dst_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS amount,
        contract_address AS ctoken,
        tx_hash,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0xa17581a9e3356d9a858b789d68b4d866e593ae94'
        AND topics [0] :: STRING = '0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb'
),
prices AS (
    SELECT
        HOUR AS block_hour,
        token_address AS token_contract,
        ctoken_address,
        AVG(price) AS token_price
    FROM
        ethereum.silver.prices
        INNER JOIN asset_details
        ON token_address = underlying_asset_address
    WHERE
        HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                comp_withdrawls
        )
    GROUP BY
        1,
        2,
        3
)
SELECT
    *,
    amount / pow(
        10,
        underlying_decimals
    ) AS supplied_base_asset,
    ROUND((amount * p.token_price) / pow(10, underlying_decimals), 2) AS supplied_base_asset_usd,
    _inserted_timestamp,
    _log_id
FROM
    comp_withdrawls
    LEFT JOIN prices p
    ON DATE_TRUNC(
        'hour',
        comp_withdrawls.block_timestamp
    ) = p.block_hour
    AND comp_withdrawls.ctoken = p.ctoken_address
    LEFT JOIN asset_details
    ON comp_withdrawls.ctoken = asset_details.ctoken_address
