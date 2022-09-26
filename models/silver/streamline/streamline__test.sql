{{ config (
    materialized = "view",
) }}

{% set get_head %}

SELECT
    streamline.udf_get_chainhead() :: INTEGER {% endset %}
    {% set results = run_query(get_head) %}
    {% if execute %}
        {# Return the first column #}
        {% set results_list = results.columns [0].values() [0] %}
    {% else %}
        {% set height = 0 %}
    {% endif %}
SELECT
    *
FROM
    TABLE(streamline.udtf_test({{ height }}))
