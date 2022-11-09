{{ config(
    materialized = 'view'
) }}

WITH base AS (

    SELECT
        DISTINCT REGEXP_REPLACE(LOWER(token_address), ' *', '') AS token_address
    FROM
        {{ source(
            'crosschain_silver',
            'ASSET_METADATA_COIN_GECKO'
        ) }}
    WHERE
        platform = 'ethereum'
    UNION
    SELECT
        DISTINCT REGEXP_REPLACE(LOWER(token_address), ' *', '') AS token_address
    FROM
        {{ source(
            'crosschain_silver',
            'ASSET_METADATA_COIN_MARKET_CAP'
        ) }}
    WHERE
        platform = 'Ethereum'
),
missing_contracts AS (
    SELECT
        *
    FROM
        base
    EXCEPT
    SELECT
        address AS token_address
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        symbol IS NOT NULL
        AND decimals IS NOT NULL
)
SELECT
    token_address,
    block_number
FROM
    missing_contracts
    JOIN {{ ref('silver__traces') }}
    ON token_address = to_address
    AND TYPE ILIKE 'create'
