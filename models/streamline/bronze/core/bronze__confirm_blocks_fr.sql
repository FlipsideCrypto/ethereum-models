{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze','core','confirm_blocks','phase_1']
) }}

SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__confirm_blocks_fr_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
   {{ ref('bronze__confirm_blocks_fr_v1') }}