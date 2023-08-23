{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

WITH base AS (

    SELECT
        tx_hash,
        l.block_number,
        l.block_timestamp,
        origin_from_address as depositor_address,
        C.symbol,
        C.name,
        C.decimals,
        event_index,
        event_name,
        decoded_flat :reserve :: STRING AS reserve_asset,
        decoded_flat :amount :: INTEGER AS amount,
        --decoded_flat :to :: STRING AS to_address,
        decoded_flat :user :: STRING AS USER,
        --decoded_flat :repayer :: STRING AS repayer,
        --decoded_flat :useATokens :: BOOLEAN AS useATokens,
        --decoded_flat :borrowRate :: INTEGER AS borrowRate,
        --decoded_flat :interestRateMode :: INTEGER AS interestRateMode,
        decoded_flat :referralCode :: INTEGER AS referralCode,
        decoded_flat :onBehalfOf :: STRING AS onBehalfOf,
        --decoded_flat :collateralAsset :: STRING AS collateralAsset,
        --decoded_flat :debtAsset :: STRING AS debtAsset,
        --decoded_flat :liquidatiedCollateralAmount :: INTEGER AS liquidatiedCollateralAmount,
        --decoded_flat :liquidator :: STRING AS liquidator,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__decoded_logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON decoded_flat :reserve :: STRING = C.address
    WHERE
        contract_address = '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2'
        AND event_name = 'Supply'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            l._inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
prices AS (
    SELECT
        HOUR AS block_hour,
        token_address AS token_contract,
        reserve_asset,
        AVG(price) AS token_price
    FROM
        {{ ref('silver__prices') }}
        INNER JOIN {{ ref('silver__aave_asset_details_v3') }}
        ON token_address = reserve_asset
    WHERE
        HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                base
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
    depositor_address,
    event_index,
    symbol AS reserve_asset_symbol,
    NAME AS reserve_asset_name,
    decimals AS reserve_asset_decimals,
    event_name,
    a.reserve_asset AS reserve_asset_address,
    amount,
    amount / pow(
        10,
        decimals
    ) AS supplied_base_asset,
    ROUND((amount * p.token_price) / pow(10, decimals), 2) AS supplied_base_asset_usd,
    USER as user,
    referralcode as referral_code,
    onbehalfof as on_behalf_of,
    _INSERTED_TIMESTAMP,
    _LOG_ID
FROM
    base A
    LEFT JOIN prices p
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = p.block_hour
    AND A.reserve_asset = p.reserve_asset
