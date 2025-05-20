{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','phase_1']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query(
    source_name = 'transactions',
    source_version = 'v2'
) }}