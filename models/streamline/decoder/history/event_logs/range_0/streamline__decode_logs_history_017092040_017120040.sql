{# Set variables #}
{%- set model_name = 'DECODED_LOGS' -%}
{%- set model_type = 'HISTORY' -%}

{%- set default_vars = fsc_evm.set_default_variables_streamline_decoder(model_name, model_type) -%}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set streamline_params = fsc_evm.set_streamline_parameters_decoder(
    model_name=model_name,
    model_type=model_type
) -%}

{%- set testing_limit = default_vars['testing_limit'] -%}

{# Log configuration details #}
{{ fsc_evm.log_streamline_details(
    model_name=model_name,
    model_type=model_type,
    testing_limit=testing_limit,
    streamline_params=streamline_params
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_logs_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = streamline_params
    ),
    fsc_utils.if_data_call_wait()],
    tags = ['streamline_decoded_logs_history_range_0']
) }}

{# Set up the range of blocks to decode #}
{% set start = this.identifier.split("_") [-2] %}
{% set stop = this.identifier.split("_") [-1] %}

{# Main query starts here #}
{{ fsc_evm.streamline_decoded_logs_requests(
    start,
    stop,
    model_type = model_type.lower(),
    testing_limit = testing_limit
) }}
