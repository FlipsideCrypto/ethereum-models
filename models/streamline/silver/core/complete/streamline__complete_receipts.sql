-- depends_on: {{ ref('bronze__streamline_receipts') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['streamline_core_complete']
) }}
{{ fsc_evm.streamline_core_complete(
    model = 'receipts'
) }}
