{{ config (
    materialized = 'view',
    tags = ['beacon']
) }}

SELECT
    partition_key,
    VALUE :"SLOT_NUMBER" :: INT AS slot_number,
    VALUE :"STATE_ID" :: STRING AS state_id,
    VALUE :"array_index" :: INT AS array_index,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__beacon_validators_fr_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number AS slot_number,
    state_id,
    array_index,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__beacon_validators_fr_v1') }}
