{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_fr_union_query(
    model = "confirmed_blocks"
) }}
