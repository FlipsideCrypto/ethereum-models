{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } }
) }}
{{ fsc_evm.stats_ez_core_metrics_hourly(
    token_address = var('STATS_TOKEN_ADDRESS')
) }}
