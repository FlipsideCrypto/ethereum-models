{{ config (
    materialized = 'view'
) }}
{{ v0_streamline_external_table_query(
    model = "eth_balances_v2",
    partition_function = "TO_NUMBER(SPLIT_PART(file_name, '/', 3))",
    balances = true
) }}
