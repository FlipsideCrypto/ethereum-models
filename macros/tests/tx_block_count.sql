{% test tx_block_count(
        model,
        column_name
) %}

SELECT 
    {{ column_name }}, 
    COUNT(DISTINCT block_number) AS num_blocks
FROM
    {{ model }}
GROUP BY {{ column_name }}
HAVING num_blocks > 1

{% endtest %}