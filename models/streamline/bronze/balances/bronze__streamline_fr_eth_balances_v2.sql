{{ config (
    materialized = 'view'
) }}
{{ v0_streamline_external_table_fr_query(
    model = "eth_balances_v2",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )",
    balances = true
) }}
