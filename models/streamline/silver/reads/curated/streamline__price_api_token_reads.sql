{{ config(
    materialized = 'view',
    tags = ['streamline_reads_curated']
) }}

WITH base AS (

    SELECT
        DISTINCT token_address
    FROM
        {{ ref(
            'silver__complete_token_asset_metadata'
        ) }}
    WHERE
        provider = 'coingecko'
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
        {{ ref('core__dim_contracts') }}
    WHERE
        symbol IS NOT NULL
        AND decimals IS NOT NULL
)
SELECT
    token_address,
    block_number
FROM
    missing_contracts
    JOIN {{ ref('core__fact_traces') }}
    ON token_address = to_address
    AND TYPE ILIKE 'create'
