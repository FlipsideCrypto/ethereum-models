{{ config (
    materialized = 'view',
    tags = ['bronze_beacon_pending_deposits']
) }}
{{ v0_streamline_external_table_fr_query(
    model = "pending_deposits",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )",
    block_number = false
) }}
