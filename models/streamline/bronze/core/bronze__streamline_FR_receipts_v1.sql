{{ config (
    materialized = 'view'
) }}
{{ fsc_utils.streamline_external_table_FR_query_v2(
    model = "receipts",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )",
    partition_column = "_partition_by_block_id"
) }}
