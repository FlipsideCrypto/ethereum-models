{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
    enabled = false
) }}

WITH aave_v3 AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        tx_hash,
        C.symbol,
        C.name,
        event_index,
        event_name,
        decoded_flat :reserve :: STRING AS reserve_asset,
        decoded_flat :amount :: INTEGER AS amount,
        decoded_flat :to :: STRING AS to_address,
        decoded_flat :user :: STRING AS USER,
        decoded_flat :repayer :: STRING AS repayer,
        decoded_flat :useATokens :: BOOLEAN AS useATokens,
        decoded_flat :borrowRate :: INTEGER AS borrowRate,
        decoded_flat :interestRateMode :: INTEGER AS interestRateMode,
        decoded_flat :referralCode :: INTEGER AS referralCode,
        decoded_flat :onBehalfOf :: STRING AS onBehalfOf,
        decoded_flat :collateralAsset :: STRING AS collateralAsset,
        decoded_flat :debtAsset :: STRING AS debtAsset,
        decoded_flat :liquidatiedCollateralAmount :: INTEGER AS liquidatiedCollateralAmount,
        decoded_flat :liquidator :: STRING AS liquidator,
        l._inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON decoded_flat :reserve :: STRING = C.address
    WHERE
        contract_address = '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2'
        AND event_name IN (
            'Supply',
            'Borrow',
            'Repay',
            'Withdraw',
            'LiquidationCall'
        )

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            l._inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    *
FROM
    aave_v3
ORDER BY
    block_number DESC
