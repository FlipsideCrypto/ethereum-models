{{ config (
    materialized = 'view'
) }}

SELECT
    VALUE,
    partition_key,
    metadata,
    DATA,
    file_name,
    inserted_timestamp
FROM
    {{ ref('bronze__streamline_FR_traces_v2') }}
UNION ALL
SELECT
    VALUE,
    _partition_by_block_id AS partition_key,
    metadata,
    DATA,
    file_name,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('bronze__streamline_FR_traces_v1') }}
