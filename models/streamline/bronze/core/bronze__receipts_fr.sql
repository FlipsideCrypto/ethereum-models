{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze','core','receipts','phase_1']
) }}

SELECT
    partition_key,
    block_number,
    array_index,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__receipts_fr_v3') }}
UNION ALL
SELECT
    partition_key,
    block_number,
    array_index,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__receipts_fr_v2') }}