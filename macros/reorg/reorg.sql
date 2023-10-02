{% macro handle_reorg(target) %}
DELETE FROM
    {{ target }}
    t
WHERE
    t._inserted_timestamp > DATEADD(
        'hour',
        -36,
        CURRENT_TIMESTAMP
    )
    AND NOT EXISTS (
        SELECT
            1
        FROM
            {{ ref('silver__transactions') }}
            s
        WHERE
            s._inserted_timestamp > DATEADD(
                'hour',
                -36,
                CURRENT_TIMESTAMP
            )
            AND s.block_number = t.block_number
            AND s.tx_hash = t.tx_hash
    );
{% endmacro %}
