{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    block_number,
    VALUE :"CONTRACT_ADDRESS" :: STRING AS contract_address,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_contract_abis_v2') }}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    contract_address,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_contract_abis_v1') }}