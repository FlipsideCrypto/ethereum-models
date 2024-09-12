{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_fr_query(
    model = "token_balances",
    partition_function = "TO_NUMBER(SPLIT_PART(file_name, '/', 3))",
    partition_join_key = "_partition_by_block_id",
    balances = true,
    block_number = false
) }}
