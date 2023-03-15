{% test recent_dupes(
    model,
    unique_key,
    date_field,
    days_back,
    error_threshold
) %}
SELECT
    {{ unique_key }},
    COUNT(*) AS records
FROM
    {{ model }}
WHERE
    {{ date_field }} > CURRENT_DATE() - {{ days_back }}
GROUP BY
    1
HAVING
    records > {{ error_threshold }}

    {% endtest %}
