{{ config (
    materialized = 'view'
) }}
{{ fsc_utils.streamline_external_table_query_v2(
    model = "token_balances_v2",
    partition_function = "TO_NUMBER(SPLIT_PART(file_name, '/', 3))",
    evm_balances = True
) }}
