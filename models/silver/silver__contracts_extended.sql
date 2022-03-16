{{ config(
    materialized = 'incremental',
    unique_key = 'address'
) }}

WITH bronze AS (

    SELECT
        system_created_at,
        block_id,
        block_timestamp,
        creator_address,
        LOWER(contract_address) AS contract_address,
        logic_address,
        token_convention,
        NAME,
        symbol,
        decimals,
        meta,
        'bronze' AS model
    FROM
        {{ ref('bronze__contracts') }}
    WHERE
        meta IS NOT NULL
        AND CASE
            WHEN meta :decimals :: STRING IS NOT NULL
            AND len(
                meta :decimals :: STRING
            ) >= 3 THEN TRUE
            ELSE FALSE
        END = FALSE
),
backfill_contracts AS (
    SELECT
        system_created_at,
        block_id,
        block_timestamp,
        creator_address,
        LOWER(contract_address) AS contract_address,
        logic_address,
        token_convention,
        NAME,
        symbol,
        decimals,
        meta,
        'backfill' AS model
    FROM
        {{ ref('bronze__contracts_backfill') }}
    WHERE
        meta IS NOT NULL
        AND CASE
            WHEN meta :decimals :: STRING IS NOT NULL
            AND len(
                meta :decimals :: STRING
            ) >= 3 THEN TRUE
            ELSE FALSE
        END = FALSE
),
legacy_contracts AS (
    SELECT
        '2000-01-01' :: TIMESTAMP AS system_created_at,
        NULL AS block_id,
        NULL AS block_timestamp,
        NULL AS creator_address,
        LOWER(address) AS contract_address,
        NULL AS logic_address,
        NULL AS token_convention,
        meta :name AS NAME,
        meta :symbol AS symbol,
        meta :decimals AS decimals,
        meta,
        'redshift' AS model
    FROM
        {{ source(
            'flipside_silver',
            'ethereum_contracts'
        ) }}
    WHERE
        CASE
            WHEN meta :decimals :: STRING IS NOT NULL
            AND len(
                meta :decimals :: STRING
            ) >= 3 THEN TRUE
            ELSE FALSE
        END = FALSE
),
full_list AS (
    SELECT
        system_created_at,
        block_id,
        block_timestamp,
        creator_address,
        contract_address,
        logic_address,
        token_convention,
        NAME,
        symbol,
        decimals,
        meta,
        model
    FROM
        bronze -- UNION
        -- SELECT
        --     system_created_at,
        --     block_id,
        --     block_timestamp,
        --     creator_address,
        --     contract_address,
        --     logic_address,
        --     token_convention,
        --     NAME,
        --     symbol,
        --     decimals,
        --     meta,
        --     model
        -- FROM
        --     backfill_contracts
        -- UNION
        -- SELECT
        --     system_created_at,
        --     block_id,
        --     block_timestamp,
        --     creator_address,
        --     contract_address,
        --     logic_address,
        --     token_convention,
        --     NAME,
        --     symbol,
        --     decimals,
        --     meta,
        --     model
        -- FROM
        --     legacy_contracts
)
SELECT
    *
FROM
    full_list
