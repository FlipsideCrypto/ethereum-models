{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze','core','phase_1']
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
    {{ ref('bronze__blocks_fr_v3') }}
UNION ALL
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