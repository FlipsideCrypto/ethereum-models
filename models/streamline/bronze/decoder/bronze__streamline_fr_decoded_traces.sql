{{ config (
    materialized = 'view'
) }}
{{ fsc_utils.streamline_external_table_FR_query_decoder(
    model = "decoded_traces"
) }}
