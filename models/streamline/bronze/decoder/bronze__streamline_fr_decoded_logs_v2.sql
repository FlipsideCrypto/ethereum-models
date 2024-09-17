{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_fr_query_decoder(
    model = "decoded_logs_v2"
) }}
