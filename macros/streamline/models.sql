{% macro decode_logs_history(
        start,
        stop
    ) %}
    WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_max_block_by_date") }}
            qualify ROW_NUMBER() over (
                ORDER BY
                    block_number DESC
            ) = 1
    )
SELECT
    l.block_number,
    l._log_id,
    abi.data AS abi,
    l.data
FROM
    {{ ref("streamline__decode_logs") }}
    l
    INNER JOIN {{ ref("silver__abis") }}
    abi
    ON l.abi_address = abi.contract_address
WHERE
    (
        l.block_number BETWEEN {{ start }}
        AND {{ stop }}
    )
    AND l.block_number <= (
        SELECT
            block_number
        FROM
            look_back
    )
    AND _log_id NOT IN (
        SELECT
            _log_id
        FROM
            {{ ref("streamline__complete_decode_logs") }}
        WHERE
            (
                block_number BETWEEN {{ start }}
                AND {{ stop }}
            )
            AND block_number <= (
                SELECT
                    block_number
                FROM
                    look_back
            )
    )
{% endmacro %}
