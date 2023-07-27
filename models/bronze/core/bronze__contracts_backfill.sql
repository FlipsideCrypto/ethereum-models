{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address'
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        creator_address,
        contract_address,
        logic_address,
        token_convention,
        TO_OBJECT(PARSE_JSON(contract_meta)) AS meta
    FROM
        {{ source(
            'flipside_silver',
            'ethereum_contracts_backfill'
        ) }}
    WHERE
        CHECK_JSON(contract_meta) IS NULL
)
SELECT
    block_id :: STRING AS block_id,
    block_timestamp :: TIMESTAMP AS block_timestamp,
    creator_address :: STRING AS creator_address,
    LOWER(
        contract_address :: STRING
    ) AS contract_address,
    logic_address :: STRING AS logic_address,
    token_convention :: STRING AS token_convention,
    meta :name :: STRING AS NAME,
    meta :symbol :: STRING AS symbol,
    meta :decimals :: INTEGER AS decimals,
    meta :: OBJECT AS meta,
    block_timestamp :: TIMESTAMP AS system_created_at
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY LOWER(contract_address)
ORDER BY
    block_timestamp DESC)) = 1
