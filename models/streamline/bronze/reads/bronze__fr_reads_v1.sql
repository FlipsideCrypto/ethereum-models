{{ config (
    materialized = 'view'
) }}
{{ fsc_utils.streamline_external_table_FR_query_v2(
    model = "reads",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_column = "TO_DATE(concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5)))"
) }}
