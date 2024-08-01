{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    VALUE :SLOT_NUMBER :: INT AS slot_number,
    VALUE :STATE_ID :: STRING AS state_id,
    VALUE :ARRAY_INDEX :: INT AS array_index,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_beacon_validators_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    slot_number,
    state_id,
    array_index,
    VALUE,
    DATA,
    NULL AS metadata,
    NULL AS file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_beacon_validators_v1') }}
