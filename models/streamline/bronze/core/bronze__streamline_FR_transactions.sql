{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_FR_transactions_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    VALUE,
    DATA,
    NULL AS metadata,
    NULL AS file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_FR_transactions_v1') }}
