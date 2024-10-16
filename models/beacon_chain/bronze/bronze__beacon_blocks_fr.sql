{{ config (
    materialized = 'view',
    tags = ['beacon']
) }}

SELECT
    partition_key,
    VALUE :"SLOT_NUMBER" :: INT AS slot_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__beacon_blocks_fr_v2') }}
UNION ALL
SELECT
    _partition_by_slot_id AS partition_key,
    slot_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__beacon_blocks_fr_v1') }}
