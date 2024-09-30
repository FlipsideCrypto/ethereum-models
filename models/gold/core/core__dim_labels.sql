{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}
{{ fsc_evm.core_dim_labels() }}