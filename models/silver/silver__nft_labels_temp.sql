{{ config(
    materialized = 'incremental',
    unique_key = 'address_token_id',
    cluster_by = ['project_address']
) }}

WITH labels AS (

    SELECT
        address AS project_address,
        label,
        1 AS rnk
    FROM
        {{ ref('core__dim_labels') }}
),
backup_meta AS (
    SELECT
        address AS project_address,
        NAME AS label,
        2 AS rnk
    FROM
        {{ source(
            'ethereum_silver',
            'token_meta_backup'
        ) }}
),
meta_union AS (
    SELECT
        project_address,
        label,
        rnk
    FROM
        labels
    UNION ALL
    SELECT
        project_address,
        label,
        rnk
    FROM
        backup_meta
),
unique_meta AS (
    SELECT
        project_address,
        label,
        rnk
    FROM
        meta_union qualify(ROW_NUMBER() over(PARTITION BY project_address
    ORDER BY
        rnk ASC)) = 1
),
token_metadata_legacy AS (
    SELECT
        LOWER(contract_address) AS contract_address,
        token_id,
        token_metadata,
        project_name AS legacy_project_name
    FROM
        {{ source(
            'flipside_gold_ethereum',
            'nft_metadata'
        ) }}
),
token_metadata AS (
    SELECT
        LOWER(address) AS address,
        NAME
    FROM
        {{ ref('silver__contracts') }}
)
SELECT
    COALESCE(
        label,
        legacy_project_name,
        token_metadata.name
    ) AS project_name,
    project_address,
    token_id,
    token_metadata,
    CONCAT(
        project_address,
        '-',
        token_id
    ) AS address_token_id
FROM
    unique_meta full
    OUTER JOIN token_metadata_legacy
    ON unique_meta.project_address = token_metadata_legacy.contract_address
    LEFT JOIN token_metadata
    ON unique_meta.project_address = token_metadata.address
