{% test hour_gaps(
        model,
        column_name
) %}

WITH gap_times AS (
  SELECT t1.{{ column_name }} AS start_time, 
         MIN(t2.{{ column_name }}) AS end_time
  FROM {{ model }} t1
  LEFT JOIN {{ model }} t2
    ON t2.{{ column_name }} > t1.{{ column_name }}
  GROUP BY 1
  ORDER BY start_time DESC
)
SELECT *
FROM gap_times
WHERE end_time IS NOT NULL 
	AND start_time::timestamp + INTERVAL '1 hour' <> end_time::timestamp

{% endtest %}