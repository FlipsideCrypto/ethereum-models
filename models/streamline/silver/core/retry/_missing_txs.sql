{{ config (
    materialized = "ephemeral"
) }}
{{ fsc_evm.retry_missing_txs() }}
