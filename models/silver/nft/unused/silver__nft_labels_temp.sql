{{ config(
    materialized = 'incremental',
    unique_key = 'address_token_id',
    cluster_by = ['project_address'],
    tags = ['stale']
) }}

WITH labels AS (

    SELECT
        address AS project_address,
        project_name AS label,
        1 AS rnk,
        insert_date
    FROM
        {{ ref('silver__labels') }}

{% if is_incremental() %}
WHERE
    insert_date >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
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

{% if is_incremental() %}
WHERE
    CURRENT_DATE = '1970-01-01'
{% endif %}
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
            'ethereum_silver',
            'nft_metadata_legacy'
        ) }}

{% if is_incremental() %}
WHERE
    system_created_at :: DATE = '1970-01-01'
{% endif %}
),
token_metadata AS (
    SELECT
        LOWER(address) AS address,
        NAME
    FROM
        {{ ref('silver__contracts') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE
        FROM
            {{ this }}
    )
{% endif %}
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
    ) AS address_token_id,
    SYSDATE() AS _inserted_timestamp
FROM
    unique_meta full
    OUTER JOIN token_metadata_legacy
    ON unique_meta.project_address = token_metadata_legacy.contract_address
    LEFT JOIN token_metadata
    ON unique_meta.project_address = token_metadata.address
