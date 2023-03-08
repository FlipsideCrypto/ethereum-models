{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_contract_abis()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}


SELECT
    contract_address,
    block_number
FROM
    {{ ref("streamline__contract_addresses") }}
WHERE
    block_number > 15000000
EXCEPT
SELECT
    contract_address,
    block_number
FROM
    {{ ref("streamline__complete_contract_abis") }}
WHERE
    block_number > 15000000
