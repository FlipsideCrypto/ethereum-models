{{ config (
    materialized = 'view'
) }}
{{ v0_streamline_external_table_fr_query_decoder(
    model = "decoded_traces_v2"
) }}
