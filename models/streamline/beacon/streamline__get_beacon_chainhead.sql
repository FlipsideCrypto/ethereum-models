{# Set variables #}
{%- set model_name = 'BEACON_CHAINHEAD' -%}
{%- set model_type = 'COMPLETE' -%}

{%- set default_vars = fsc_evm.set_default_variables_streamline(model_name, model_type) -%}

{%- set node_url = default_vars['node_url'] -%}
{%- set node_secret_path = default_vars['node_secret_path'] -%}

{# Set up dbt configuration #}
{{ config (
    materialized = 'table',
    tags = ['streamline_beacon_complete']
) }}

{# Main query starts here #}
SELECT
    {{ target.database }}.live.udf_api(
        'GET',
        '{{ node_url }}/eth/v1/beacon/headers',
        OBJECT_CONSTRUCT(
            'accept',
            'application/json'
        ),
        NULL,
        '{{ node_secret_path }}'
    ) AS resp,
    resp :data :data [0] :header :message :slot :: INT AS slot_number
