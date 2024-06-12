{{ config(
    materialized = 'incremental',
    tags = ['curated']
) }}

WITH market_api AS (

    SELECT
        live.udf_api(
            'POST',
            'https://blue-api.morpho.org/graphql',
            OBJECT_CONSTRUCT(
                'Accept-Encoding',
                'gzip, deflate, br',
                'Content-Type',
                'application/json',
                'Accept',
                'application/json',
                'Connection',
                'keep-alive',
                'DNT',
                '1',
                'Origin',
                'https://blue-api.morpho.org'
            ),
            OBJECT_CONSTRUCT(
                'query',
                'query { markets { items { uniqueKey lltv oracleAddress irmAddress loanAsset { address symbol decimals } collateralAsset { address symbol decimals } state { borrowApy borrowAssets borrowAssetsUsd supplyApy supplyAssets supplyAssetsUsd fee utilization } } } }'
            )
        ) AS read_output,
        SYSDATE() AS _inserted_timestamp
),
market_flatten as (
    SELECT
        VALUE :collateralAsset :address :: STRING AS collateral_address,
        VALUE :collateralAsset :decimals :: INTEGER AS collateral_decimals,
        VALUE :collateralAsset :symbol :: STRING AS collateral_symbol,
        VALUE :loanAsset :address :: STRING AS loan_address,
        VALUE :loanAsset :decimals :: INTEGER AS loan_decimals,
        VALUE :loanAsset :symbol :: STRING AS loan_symbols,
        VALUE :irmAddress :: STRING AS irm_address,
        VALUE :oracleAddress :: STRING AS oracle_address,
        VALUE :lltv :: INTEGER AS lltv,
        VALUE :uniqueKey :: STRING AS market_id,
        VALUE,
        _inserted_timestamp
    FROM
        market_api,
        LATERAL FLATTEN(
            input => read_output :data :data :markets :items
        )
)
SELECT
    collateral_address,
    collateral_symbol,
    collateral_decimals,
    loan_address,
    loan_decimals,
    loan_symbols,
    irm_address,
    oracle_address,
    lltv,
    market_id,
    _inserted_timestamp
FROM
    market_flatten
{% if is_incremental() %}
WHERE
    market_id not in (SELECT market_id from {{this}})
{% endif %}