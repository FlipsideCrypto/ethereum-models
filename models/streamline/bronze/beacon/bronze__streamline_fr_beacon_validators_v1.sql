{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_FR_query(
    "beacon_validators",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "block_number"
) }}
