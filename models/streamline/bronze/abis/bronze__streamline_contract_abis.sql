{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_query(
    model = "contract_abis_v2",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)"
) }}
