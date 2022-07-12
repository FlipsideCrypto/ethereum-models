{{ config (
    materialized = "view",
    primary_key = "id",
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
