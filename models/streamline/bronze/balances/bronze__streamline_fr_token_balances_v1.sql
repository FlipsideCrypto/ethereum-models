{{ config (
    materialized = 'view'
) }}
{{ fsc_utils.streamline_external_table_FR_query_v2(
    model = "token_balances",
    partition_function = "TO_NUMBER(SPLIT_PART(file_name, '/', 3))",
    partition_column = "_partition_by_block_id",
    evm_balances = True
) }}
