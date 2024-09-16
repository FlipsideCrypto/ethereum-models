-- depends_on: {{ ref('bronze__streamline_decoded_logs') }}
{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_update_columns = ["_log_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_log_id)",
    tags = ['streamline_decoded_logs_complete']
) }}
{{ fsc_evm.streamline_decoded_complete(
    model = 'decoded_logs'
) }}
