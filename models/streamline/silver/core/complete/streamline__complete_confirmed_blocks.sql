-- depends_on: {{ ref('bronze__streamline_confirmed_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    tags = ['streamline_core_complete']
) }}
{{ fsc_evm.streamline_core_complete(
    model = 'confirmed_blocks'
) }}
