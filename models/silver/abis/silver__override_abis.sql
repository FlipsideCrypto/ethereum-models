{{ config(
    materialized = 'view',
    tags = ['abi']
) }}

SELECT
    NULL AS contract_address,
    NULL AS DATA
