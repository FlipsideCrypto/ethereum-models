{{ config(
    materialized = 'view',
    tags = ['contract_abi']
) }}

SELECT
    NULL AS contract_address,
    NULL AS DATA
