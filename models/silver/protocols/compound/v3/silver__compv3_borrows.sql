{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH borrow AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS asset,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS src_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS borrow_amount,
        origin_from_address AS borrower_address,
        'Compound V3' AS compound_version,
        C.compound_market_name AS NAME,
        C.compound_market_symbol AS symbol,
        C.compound_market_decimals AS decimals,
        C.underlying_asset_address,
        C.underlying_asset_symbol,
        'ethereum' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__compv3_asset_details') }} C
        ON asset = C.compound_market_address
    WHERE
        topics [0] = '0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb' --withdrawl
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
prices AS (
    SELECT
        HOUR AS block_hour,
        token_address AS token_contract,
        compound_market_address,
        AVG(price) AS token_price
    FROM
        {{ ref('price__ez_hourly_token_prices') }}
        INNER JOIN {{ ref('silver__compv3_asset_details') }}
        ON token_address = underlying_asset_address
    WHERE
        HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                borrow
        )
    GROUP BY
        1,
        2,
        3
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    w.asset AS compound_market,
    borrower_address,
    w.underlying_asset_address AS borrowed_token,
    borrow_amount / pow(
        10,
        w.decimals
    ) AS borrowed_tokens,
    borrow_amount * token_price / pow(
        10,
        w.decimals
    ) AS borrowed_usd,
    w.symbol as ctoken_symbol,
    compound_version,
    w.underlying_asset_symbol AS symbol,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    borrow w
    LEFT JOIN prices p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = block_hour
    AND w.asset = p.compound_market_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
