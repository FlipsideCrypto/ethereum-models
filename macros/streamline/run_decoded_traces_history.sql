{% macro run_decoded_traces_history() %}

{% set blockchain = var('GLOBAL_PROD_DB_NAME','ethereum').lower() %}

{% set check_for_new_user_abis_query %}
    select 1
    from {{ ref('silver__user_verified_abis') }}
    where _inserted_timestamp::date = sysdate()::date
{% endset %}

{% set results = run_query(check_for_new_user_abis_query) %}

{% if execute %}
    {% set new_user_abis = results.columns[0].values()[0] %}

    {% if new_user_abis %}
        {% set invoke_workflow_query %}
            SELECT
                github_actions.workflow_dispatches(
                    'FlipsideCrypto',
                    '{{ blockchain }}' ~ '-models',
                    'dbt_run_streamline_decoded_traces_history.yml',
                    NULL
                )
        {% endset %}

        {% do run_query(invoke_workflow_query) %}
    {% endif %}
{% endif %}
{% endmacro %}