{{ config (
    materialized = "view",
    tags = ['streamline_core_complete']
) }}
{{ fsc_evm.block_sequence() }}
