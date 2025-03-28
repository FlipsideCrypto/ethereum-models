{{ config (
    materialized = 'view'
) }}
{{ v0_streamline_external_table_fr_query(
    model = "contract_abis",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_join_key = "_partition_by_block_id",
    block_number = false
) }}
