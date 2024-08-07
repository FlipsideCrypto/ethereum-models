{{ config (
    materialized = 'view'
) }}

SELECT
    partition_key,
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    VALUE :CONTRACT_ADDRESS :: STRING AS contract_address,
    VALUE :FUNCTION_SIGNATURE :: STRING AS function_signature,
    VALUE :FUNCTION_INPUT :: STRING AS function_input,
    VALUE :CALL_NAME :: STRING AS call_name,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_reads_v2') }}
UNION ALL
SELECT
    _partition_by_modified_date AS partition_key,
    block_number,
    contract_address,
    function_signature,
    function_input,
    call_name,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__streamline_fr_reads_v1') }}