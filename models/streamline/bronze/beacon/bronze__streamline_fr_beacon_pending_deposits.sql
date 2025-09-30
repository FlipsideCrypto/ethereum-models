{{ config (
    materialized = 'view',
    tags = ['bronze_beacon_pending_deposits']
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
    {{ ref('bronze__streamline_fr_beacon_pending_deposits_v2') }}
