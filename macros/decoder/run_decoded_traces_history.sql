{% macro run_decoded_traces_history() %}
    {% set check_for_new_user_abis_query %}
SELECT
    1
FROM
    {{ ref('silver__user_verified_abis') }}
WHERE
    _inserted_timestamp :: DATE = SYSDATE() :: DATE
    AND DAYNAME(SYSDATE()) <> 'Sat' {% endset %}
    {% set results = run_query(check_for_new_user_abis_query) %}
    {% if execute %}
        {% set new_user_abis = results.columns [0].values() [0] %}
        {% if new_user_abis %}
            {% set invoke_traces_query %}
        SELECT
            github_actions.workflow_dispatches(
                'FlipsideCrypto',
                '{{ blockchain }}' || '-models',
                'dbt_run_streamline_decoded_traces_history.yml',
                NULL
            ) {% endset %}
            {% do run_query(invoke_traces_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}
