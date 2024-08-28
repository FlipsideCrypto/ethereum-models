{{ config (
    materialized = "ephemeral"
) }}
{{ fsc_evm.block_lookback_72_hour() }}
