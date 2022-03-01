{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    incremental_strategy = 'delete+insert',
    cluster_by = ['address'],
    tags = ['snowflake', 'ethereum', 'silver_ethereum','ethereum_contracts']
) }}

SELECT
    LOWER(address) :: STRING AS address,
    meta :symbol :: STRING AS symbol,
    meta :name :: STRING AS NAME,
    meta :decimals :: INTEGER AS decimals,
    meta AS contract_metadata
FROM
    {{ source(
        'flipside_silver',
        'contracts'
    ) }}
