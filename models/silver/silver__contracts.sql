{{ config(
    materialized = 'incremental',
    unique_key = 'address'
) }}

SELECT
    LOWER(address) :: STRING AS address,
    meta :symbol :: STRING AS symbol,
    meta :name :: STRING AS NAME,
    meta :decimals :: INTEGER AS decimals,
    meta AS contract_metadata
FROM
    {{ source(
        'flipside_silver_ethereum',
        'contracts'
    ) }}
WHERE
    meta IS NOT NULL
