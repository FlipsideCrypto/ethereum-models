{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}
{{ fsc_evm.block_ranges() }}
