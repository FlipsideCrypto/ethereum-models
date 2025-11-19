{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    block_number,
    VALUE :"ADDRESS" :: STRING AS address,
    block_timestamp,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_eth_balances_v2') }}
