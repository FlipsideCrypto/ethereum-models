{{ config (
    materialized = "ephemeral"
) }}
{{ fsc_evm.retry_unconfirmed_blocks() }}
