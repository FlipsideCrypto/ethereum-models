{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','receipts','phase_1']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query_fr(
    source_name = 'receipts',
    source_version = 'v2'
) }}