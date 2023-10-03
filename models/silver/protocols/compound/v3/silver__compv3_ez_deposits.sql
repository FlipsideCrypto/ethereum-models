{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

WITH supply AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS compound_market,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS asset,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS supply_amount,
        origin_from_address AS depositor_address,
        'Compound V3' AS compound_version,
        c.address as underlying_asset_address,
        C.name,
        C.symbol,
        C.decimals,
        'ethereum' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ref('silver__logs')}}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON asset = address
    WHERE
        topics [0] = '0xfa56f7b24f17183d81894d3ac2ee654e3c26388d17a28dbd9549b8114304e1f4' --SupplyCollateral
{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

),
prices AS (
    SELECT
        *
    FROM
        {{ ref('silver__compv3_token_prices') }}
    WHERE
        prices_hour :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                supply
        )
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    compound_market,
    w.asset as protocol_token,
    w.underlying_asset_address as deposit_asset,
    supply_amount / pow(
        10,
        w.decimals
    ) AS supply_tokens,
    supply_amount * hourly_price / pow(
        10,
        w.decimals
    ) AS supply_usd,
    depositor_address,
    compound_version,
    w.name,
    w.symbol,
    w.decimals,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    supply w
    LEFT JOIN prices p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = prices_hour
    AND w.asset = p.asset qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
