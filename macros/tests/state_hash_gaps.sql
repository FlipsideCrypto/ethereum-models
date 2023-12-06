{% test state_hash_gaps(
    model
) %}
WITH batch_check AS (
    SELECT
        state_batch_index,
        state_max_block,
        LAG(
            state_max_block,
            1
        ) over (
            ORDER BY
                state_batch_index
        ) AS previous_state_max_block,
        state_max_block - LAG(
            state_max_block,
            1
        ) over (
            ORDER BY
                state_batch_index
        ) AS difference
    FROM
        {{ model }}
)
SELECT
    *
FROM
    batch_check
WHERE
    difference <> 1800 
{% endtest %}
