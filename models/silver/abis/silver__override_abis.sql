{{ config(
    materialized = 'view',
    tags = ['abis']
) }}

SELECT
    NULL AS contract_address,
    NULL AS DATA
