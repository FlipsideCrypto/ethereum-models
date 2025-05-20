{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','decoded_logs','phase_2']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query_decoder_fr(
    source_name = 'decoded_logs',
    source_version = 'v2'
) }}