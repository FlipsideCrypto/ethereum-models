{{ config (
    materialized = 'view'
) }}
{{ v0_streamline_external_table_fr_query(
    model = "reads",
    partition_function = "TO_DATE(concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5)))",
    partition_join_key = "_partition_by_modified_date",
    block_number = false
) }}
