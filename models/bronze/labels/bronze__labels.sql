{{ config(
    materialized = 'view'
) }}
{{ fsc_evm.bronze_labels() }}