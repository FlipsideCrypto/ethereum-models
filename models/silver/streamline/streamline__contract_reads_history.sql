{{ config (
    materialized = "view",
    primary_key = "id",
    post_hook = "call {{this.schema}}.sp_get_{{this.identifier}}()"
) }}

SELECT
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    {{ ref("streamline__contract_reads") }}
WHERE
    block_number <= 15000000
EXCEPT
SELECT
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input
FROM
    {{ ref("streamline__complete_contract_reads") }}
WHERE
    block_number <= 15000000
