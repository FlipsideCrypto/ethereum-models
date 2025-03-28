{{ config (
    materialized = 'view',
    tags = ['bronze_core']
) }}

SELECT
    partition_key,
    block_number,
    VALUE,
    CASE 
        WHEN DATA :result IS NULL THEN DATA
        ELSE DATA :result
    END AS DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__blocks_fr_v2') }}
{% if var('GLOBAL_USES_STREAMLINE_V1', false) %}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    VALUE,
    DATA :result AS DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
   {{ ref('bronze__blocks_fr_v1') }}
{% endif %}