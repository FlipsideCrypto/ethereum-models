{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_query(
    model = "reads_v2",
    partition_function = "TO_DATE(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1))"
) }}