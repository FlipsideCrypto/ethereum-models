-- depends_on: {{ ref('bronze__streamline_decoded_traces') }}
{{ config (
    materialized = "incremental",
    unique_key = "_call_id",
    cluster_by = "ROUND(block_number, -3)",
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_update_columns = ["_call_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_call_id)",
    tags = ['streamline_decoded_traces_complete']
) }}
{{ fsc_evm.streamline_decoded_complete(
    decoded_traces = true
) }}
