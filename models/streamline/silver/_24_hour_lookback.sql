{{ config (
    materialized = "ephemeral"
) }}
{{ fsc_evm.block_lookback_24_hour() }}
