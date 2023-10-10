{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS set_address,
        SUBSTR(
            input_data,
            139
        ) AS name_part,
        utils.udf_hex_to_string(
            name_part :: STRING
        ) AS set_ens_name,
        REPLACE(
            set_ens_name,
            '.',
            ''
        ) AS set_ens_name_clean2,
        SPLIT_PART(
            set_ens_name,
            '.',
            1
        ) AS part1,
        SPLIT_PART(
            set_ens_name,
            '.',
            2
        ) AS part2,
        NULLIF(SPLIT_PART(set_ens_name, '.', 3), '') AS part3,
        NULLIF(SPLIT_PART(set_ens_name, '.', 4), '') AS part4,
        CASE
            WHEN part4 IS NOT NULL THEN part1
            ELSE NULL
        END AS subdomain2,
        CASE
            WHEN part4 IS NOT NULL THEN part2
            WHEN part4 IS NULL
            AND part3 IS NOT NULL THEN part1
            ELSE NULL
        END AS subdomain1,
        CASE
            WHEN part4 IS NOT NULL THEN part3
            WHEN part4 IS NULL
            AND part3 IS NOT NULL THEN part2
            WHEN part4 IS NULL
            AND part3 IS NULL
            AND part2 IS NOT NULL THEN part1
        END AS second_level_domain,
        CASE
            WHEN part4 IS NOT NULL THEN part4
            WHEN part4 IS NULL
            AND part3 IS NOT NULL THEN part3
            WHEN part4 IS NULL
            AND part3 IS NULL
            AND part2 IS NOT NULL THEN part2
        END AS top_level_domain,
        CONCAT(COALESCE(subdomain2,''),COALESCE(subdomain1,''),second_level_domain) AS set_ens_name_clean,
        tx_status,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_status = 'SUCCESS'
        AND origin_function_signature = '0xc47f0027'
        AND to_address IN (
            '0x9062c0a6dbd6108336bcbe4593a3d1ce05512069',
            -- ENS: Old Reverse Registrar
            '0x084b1c3c81545d370f3634392de611caabff8148',
            -- ENS: Old Reverse Registrar 2
            '0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb' -- ENS: Reverse Registrar
        )
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    set_address,
    set_ens_name,
    set_ens_name_clean,
    set_ens_name_clean2,
    subdomain2,
    subdomain1,
    second_level_domain,
    top_level_domain,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY set_address
ORDER BY
    block_timestamp DESC)) = 1
