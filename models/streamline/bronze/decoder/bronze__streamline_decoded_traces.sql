{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_query_decoder(
    model = "decoded_traces"
) }}
