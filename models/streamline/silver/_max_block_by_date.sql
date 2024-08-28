{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}
{{ fsc_evm.max_block_by_date() }}
