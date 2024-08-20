{{ config (
    materialized = 'view'
) }}
{{ fsc_utils.streamline_external_table_query_decoder(
    model = "decoded_logs"
) }}
