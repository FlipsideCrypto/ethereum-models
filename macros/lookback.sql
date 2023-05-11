{% macro lookback() %}
    {% if execute and is_incremental() %}
        {% set query %}
    SELECT
        MAX(_inserted_timestamp) :: DATE - 3
    FROM
        {{ this }};
{% endset %}
        {% set last_week = run_query(query).columns [0] [0] %}
        {% do return(last_week) %}
    {% endif %}
{% endmacro %}
