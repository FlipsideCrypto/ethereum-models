{{ config (
    materialized = "ephemeral"
) }}
{{ fsc_evm.max_block_by_hour() }}
