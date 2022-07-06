{{ config (
    materialized = "view",
    primary_key = "id",
    post_hook = "call {{this.schema}}.sp_get_{{this.identifier}}()"
) }}

SELECT
    block_number,
    address,
    contract_address
FROM
    {{ ref("streamline__token_balances_by_date") }}
WHERE
    block_number <= 15000000
EXCEPT
SELECT
    block_number,
    address,
    contract_address
FROM
    {{ ref("streamline__complete_token_balances") }}
WHERE
    block_number <= 15000000
