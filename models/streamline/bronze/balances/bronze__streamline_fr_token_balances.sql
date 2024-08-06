{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    VALUE :"BLOCK_NUMBER" :: INT AS block_number,
    VALUE :"ADDRESS" :: STRING AS address,
    VALUE :"CONTRACT_ADDRESS" :: STRING AS contract_address,
    block_timestamp,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_token_balances_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    address,
    contract_address,
    block_timestamp,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_token_balances_v1') }}
