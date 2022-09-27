{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

SELECT
    block_number,
    'committees' AS type,
    'head' AS state_id,
    SYSDATE() AS _inserted_timestamp
FROM
    {{ ref('streamline__beacon_blocks') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND (
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        '1900-01-01'
    )
)
{% endif %}

UNION ALL 

SELECT
    block_number,
    'sync_committees' AS type,
    block_number AS state_id,
    SYSDATE() AS _inserted_timestamp
FROM
    {{ ref('streamline__beacon_blocks') }}
WHERE
    1 = 1

{% if is_incremental() %}
AND (
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        '1900-01-01'
    )
)
{% endif %}
