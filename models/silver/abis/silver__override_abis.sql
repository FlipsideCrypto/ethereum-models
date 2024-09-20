{{ config(
    materialized = 'view',
    tags = ['abis']
) }}
{{ fsc_evm.silver_override_abis () }}
{# SELECT
NULL AS contract_address,
NULL AS DATA #}
